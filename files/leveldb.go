package files

import (
	"bytes"
	"encoding/binary"
	"sort"
	"time"

	"github.com/calmh/syncthing/lamport"
	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	keyTypeNode = iota
	keyTypeGlobal
)

type fileVersion struct {
	version uint64
	node    []byte
}

type versionList struct {
	versions []fileVersion
}

type fileList []scanner.File

func (l fileList) Len() int {
	return len(l)
}

func (l fileList) Swap(a, b int) {
	l[a], l[b] = l[b], l[a]
}

func (l fileList) Less(a, b int) bool {
	return l[a].Name < l[b].Name
}

type dbReader interface {
	Get([]byte, *opt.ReadOptions) ([]byte, error)
}

type dbWriter interface {
	Put([]byte, []byte)
	Delete([]byte)
}

/*

keyTypeNode (1 byte)
    repository (64 bytes)
        node (32 bytes)
            name (variable size)
            	|
				<8 byte timestamp, scanner.File>

keyTypeGlobal (1 byte)
	repository (64 bytes)
		name (variable size)
			|
			[]fileVersion (sorted)

*/

func nodeKey(repo, node, file []byte) []byte {
	k := make([]byte, 1+64+32+len(file))
	k[0] = keyTypeNode
	copy(k[1:], []byte(repo))
	copy(k[1+64:], node[:])
	copy(k[1+64+32:], []byte(file))
	return k
}

func globalKey(repo, file []byte) []byte {
	k := make([]byte, 1+64+len(file))
	k[0] = keyTypeGlobal
	copy(k[1:], []byte(repo))
	copy(k[1+64:], []byte(file))
	return k
}

func nodeKeyName(key []byte) []byte {
	return key[1+64+32:]
}

func globalKeyName(key []byte) []byte {
	return key[1+64:]
}

type deletionHandler func(db dbReader, batch dbWriter, repo, node, name []byte, dbi iterator.Iterator) uint64

type fileIterator func(f scanner.File) bool

func ldbGenericReplace(db *leveldb.DB, repo, node []byte, fs []scanner.File, deleteFn deletionHandler) uint64 {
	sort.Sort(fileList(fs)) // sort list on name, same as on disk

	start := nodeKey(repo, node, nil)                            // before all repo/node files
	limit := nodeKey(repo, node, []byte{0xff, 0xff, 0xff, 0xff}) // after all repo/node files

	batch := new(leveldb.Batch)
	snap, err := db.GetSnapshot()
	if err != nil {
		panic(err)
	}
	defer snap.Release()
	dbi := snap.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	defer dbi.Release()

	moreDb := dbi.Next()
	fsi := 0
	var newTs uint64

	for {
		var newName, oldName []byte
		moreFs := fsi < len(fs)

		if !moreDb && !moreFs {
			break
		}

		if !moreFs && deleteFn == nil {
			// We don't have any more updated files to process and deletion
			// has not been requested, so we can exit early
			break
		}

		if moreFs {
			newName = []byte(fs[fsi].Name)
		}

		if moreDb {
			oldName = nodeKeyName(dbi.Key())
		}

		cmp := bytes.Compare(newName, oldName)

		if debug {
			l.Debugf("generic replace; repo=%q node=%x moreFs=%v moreDb=%v cmp=%d newName=%q oldName=%q", repo, node, moreFs, moreDb, cmp, newName, oldName)
		}

		switch {
		case moreFs && (!moreDb || cmp == -1):
			// Disk is missing this file. Insert it.
			newTs = ldbInsert(batch, repo, node, newName, fs[fsi])
			ldbUpdateGlobal(snap, batch, repo, node, newName, fs[fsi].Version)
			fsi++

		case cmp == 0:
			// File exists on both sides - compare versions.
			var ef scanner.File
			bs := dbi.Value()
			ef.UnmarshalXDR(bs[8:])
			if fs[fsi].Version > ef.Version {
				newTs = ldbInsert(batch, repo, node, newName, fs[fsi])
				ldbUpdateGlobal(snap, batch, repo, node, newName, fs[fsi].Version)
			}
			// Iterate both sides.
			fsi++
			moreDb = dbi.Next()

		case moreDb && (!moreFs || cmp == 1):
			if deleteFn != nil {
				if ts := deleteFn(snap, batch, repo, node, oldName, dbi); ts > 0 {
					newTs = ts
				}
			}
			moreDb = dbi.Next()
		}
	}

	err = db.Write(batch, nil)
	if err != nil {
		panic(err)
	}

	return newTs
}

func ldbReplace(db *leveldb.DB, repo, node []byte, fs []scanner.File) uint64 {
	return ldbGenericReplace(db, repo, node, fs, func(db dbReader, batch dbWriter, repo, node, name []byte, dbi iterator.Iterator) uint64 {
		// Disk has files that we are missing. Remove it.
		if debug {
			l.Debugf("delete; repo=%q node=%x name=%q", repo, node, name)
		}
		batch.Delete(dbi.Key())
		ldbRemoveFromGlobal(db, batch, repo, node, name)
		return 0
	})
}

func ldbReplaceWithDelete(db *leveldb.DB, repo, node []byte, fs []scanner.File) uint64 {
	return ldbGenericReplace(db, repo, node, fs, func(db dbReader, batch dbWriter, repo, node, name []byte, dbi iterator.Iterator) uint64 {
		var f scanner.File
		bs := dbi.Value()
		err := f.UnmarshalXDR(bs[8:])
		if err != nil {
			panic(err)
		}
		if !protocol.IsDeleted(f.Flags) {
			if debug {
				l.Debugf("mark deleted; repo=%q node=%x name=%q", repo, node, name)
			}
			f.Blocks = nil
			f.Version = lamport.Default.Tick(f.Version)
			f.Flags |= protocol.FlagDeleted
			ts := ldbInsert(batch, repo, node, name, f)
			ldbUpdateGlobal(db, batch, repo, node, nodeKeyName(dbi.Key()), f.Version)
			return ts
		}
		return 0
	})
}

func ldbUpdate(db *leveldb.DB, repo, node []byte, fs []scanner.File) uint64 {
	batch := new(leveldb.Batch)
	snap, err := db.GetSnapshot()
	if err != nil {
		panic(err)
	}
	defer snap.Release()

	var newTs uint64
	for _, f := range fs {
		name := []byte(f.Name)
		fk := nodeKey(repo, node, name)
		bs, err := snap.Get(fk, nil)
		if err == leveldb.ErrNotFound {
			newTs = ldbInsert(batch, repo, node, name, f)
			ldbUpdateGlobal(snap, batch, repo, node, name, f.Version)
			continue
		}

		var ef scanner.File
		err = ef.UnmarshalXDR(bs[8:])
		if err != nil {
			panic(err)
		}
		if ef.Version != f.Version {
			newTs = ldbInsert(batch, repo, node, name, f)
			ldbUpdateGlobal(snap, batch, repo, node, name, f.Version)
		}
	}

	err = db.Write(batch, nil)
	if err != nil {
		panic(err)
	}

	return newTs
}

func ldbInsert(batch dbWriter, repo, node, name []byte, file scanner.File) uint64 {
	if debug {
		l.Debugf("insert; repo=%q node=%x %v", repo, node, file)
	}

	buf := make([]byte, 8, 128)
	ts := uint64(time.Now().UnixNano())
	binary.BigEndian.PutUint64(buf, ts)
	buf = file.AppendXDR(buf)
	nk := nodeKey(repo, node, name)
	batch.Put(nk, buf)
	return ts
}

// ldbUpdateGlobal adds this node+version to the version list for the given
// file. If the node is already present in the list, the version is updated.
// If the file does not have an entry in the global list, it is created.
func ldbUpdateGlobal(db dbReader, batch dbWriter, repo, node, file []byte, version uint64) {
	if debug {
		l.Debugf("update global; repo=%q node=%x file=%q version=%d", repo, node, file, version)
	}
	gk := globalKey(repo, file)
	svl, err := db.Get(gk, nil)
	if err != nil && err != leveldb.ErrNotFound {
		panic(err)
	}

	var fl versionList
	nv := fileVersion{
		node:    node,
		version: version,
	}
	if svl != nil {
		err = fl.UnmarshalXDR(svl)
		if err != nil {
			panic(err)
		}

		for i := range fl.versions {
			if bytes.Compare(fl.versions[i].node, node) == 0 {
				if fl.versions[i].version == version {
					// No need to do anything
					return
				}
				fl.versions = append(fl.versions[:i], fl.versions[i+1:]...)
				break
			}
		}
	}

	for i := range fl.versions {
		if fl.versions[i].version <= version {
			t := append(fl.versions, fileVersion{})
			copy(t[i+1:], t[i:])
			t[i] = nv
			fl.versions = t
			goto done
		}
	}

	fl.versions = append(fl.versions, nv)

done:
	batch.Put(gk, fl.MarshalXDR())
}

// ldbRemoveFromGlobal removes the node from the global version list for the
// given file. If the version list is empty after this, the file entry is
// removed entirely.
func ldbRemoveFromGlobal(db dbReader, batch dbWriter, repo, node, file []byte) {
	if debug {
		l.Debugf("remove from global; repo=%q node=%x file=%q", repo, node, file)
	}

	gk := globalKey(repo, file)
	svl, err := db.Get(gk, nil)
	if err != nil {
		panic(err)
	}

	var fl versionList
	err = fl.UnmarshalXDR(svl)
	if err != nil {
		panic(err)
	}

	for i := range fl.versions {
		if bytes.Compare(fl.versions[i].node, node) == 0 {
			fl.versions = append(fl.versions[:i], fl.versions[i+1:]...)
			break
		}
	}

	if len(fl.versions) == 0 {
		batch.Delete(gk)
	} else {
		batch.Put(gk, fl.MarshalXDR())
	}
}

func ldbWithHave(db *leveldb.DB, repo, node []byte, since uint64, fn fileIterator) uint64 {
	start := nodeKey(repo, node, nil)                            // before all repo/node files
	limit := nodeKey(repo, node, []byte{0xff, 0xff, 0xff, 0xff}) // after all repo/node files
	snap, err := db.GetSnapshot()
	if err != nil {
		panic(err)
	}
	defer snap.Release()
	dbi := snap.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	defer dbi.Release()

	var maxTs uint64
	for dbi.Next() {
		var f scanner.File
		bs := dbi.Value()
		ts := binary.BigEndian.Uint64(bs[:8])
		if ts > maxTs {
			maxTs = ts
		}
		if ts > since {
			err := f.UnmarshalXDR(bs[8:])
			if err != nil {
				panic(err)
			}
			if cont := fn(f); !cont {
				return maxTs
			}
		}
	}
	return maxTs
}

func ldbWithAll(db *leveldb.DB, repo []byte, fn func(ts uint64, node []byte, f scanner.File)) {
	start := nodeKey(repo, nil, nil)                                                                                                                                                                                                                               // before all repo/node files
	limit := nodeKey(repo, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, []byte{0xff, 0xff, 0xff, 0xff}) // after all repo/node files
	snap, err := db.GetSnapshot()
	if err != nil {
		panic(err)
	}
	defer snap.Release()
	dbi := snap.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	defer dbi.Release()

	for dbi.Next() {
		var f scanner.File
		node := dbi.Key()[65 : 65+32]
		bs := dbi.Value()
		ts := binary.BigEndian.Uint64(bs[:8])
		err := f.UnmarshalXDR(bs[8:])
		if err != nil {
			panic(err)
		}
		fn(ts, node, f)
	}
}

func ldbGet(db *leveldb.DB, repo, node, file []byte) scanner.File {
	nk := nodeKey(repo, node, file)
	bs, err := db.Get(nk, nil)
	if err == leveldb.ErrNotFound {
		return scanner.File{}
	}
	if err != nil {
		panic(err)
	}

	var f scanner.File
	err = f.UnmarshalXDR(bs[8:])
	if err != nil {
		panic(err)
	}
	return f
}

func ldbGetGlobal(db *leveldb.DB, repo, file []byte) scanner.File {
	k := globalKey(repo, file)
	snap, err := db.GetSnapshot()
	if err != nil {
		panic(err)
	}
	defer snap.Release()

	bs, err := snap.Get(k, nil)
	if err == leveldb.ErrNotFound {
		return scanner.File{}
	}
	if err != nil {
		panic(err)
	}

	var vl versionList
	err = vl.UnmarshalXDR(bs)
	if err != nil {
		panic(err)
	}
	if len(vl.versions) == 0 {
		l.Debugln(k)
		panic("no versions?")
	}

	k = nodeKey(repo, vl.versions[0].node, file)
	bs, err = snap.Get(k, nil)
	if err != nil {
		panic(err)
	}

	var f scanner.File
	err = f.UnmarshalXDR(bs[8:])
	if err != nil {
		panic(err)
	}
	return f
}

func ldbWithGlobal(db *leveldb.DB, repo []byte, fn fileIterator) {
	start := globalKey(repo, nil)
	limit := globalKey(repo, []byte{0xff, 0xff, 0xff, 0xff})
	snap, err := db.GetSnapshot()
	if err != nil {
		panic(err)
	}
	defer snap.Release()
	dbi := snap.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	defer dbi.Release()

	for dbi.Next() {
		var vl versionList
		err := vl.UnmarshalXDR(dbi.Value())
		if err != nil {
			panic(err)
		}
		if len(vl.versions) == 0 {
			l.Debugln(dbi.Key())
			panic("no versions?")
		}

		fk := nodeKey(repo, vl.versions[0].node, globalKeyName(dbi.Key()))
		bs, err := snap.Get(fk, nil)
		if err != nil {
			panic(err)
		}

		var f scanner.File
		err = f.UnmarshalXDR(bs[8:])
		if err != nil {
			panic(err)
		}

		if cont := fn(f); !cont {
			return
		}
	}
}

func ldbAvailability(db *leveldb.DB, repo, file []byte) []protocol.NodeID {
	k := globalKey(repo, file)
	bs, err := db.Get(k, nil)
	if err == leveldb.ErrNotFound {
		return nil
	}
	if err != nil {
		panic(err)
	}

	var vl versionList
	err = vl.UnmarshalXDR(bs)
	if err != nil {
		panic(err)
	}

	var nodes []protocol.NodeID
	for _, v := range vl.versions {
		if v.version != vl.versions[0].version {
			break
		}
		var n protocol.NodeID
		copy(n[:], v.node)
		nodes = append(nodes, n)
	}

	return nodes
}

func ldbWithNeed(db *leveldb.DB, repo, node []byte, fn fileIterator) {
	start := globalKey(repo, nil)
	limit := globalKey(repo, []byte{0xff, 0xff, 0xff, 0xff})
	snap, err := db.GetSnapshot()
	if err != nil {
		panic(err)
	}
	defer snap.Release()
	dbi := snap.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	defer dbi.Release()

	for dbi.Next() {
		var vl versionList
		err := vl.UnmarshalXDR(dbi.Value())
		if err != nil {
			panic(err)
		}
		if len(vl.versions) == 0 {
			l.Debugln(dbi.Key())
			panic("no versions?")
		}

		have := false // If we have the file, any version
		need := false // If we have a lower version of the file
		var haveVersion uint64
		for _, v := range vl.versions {
			if bytes.Compare(v.node, node) == 0 {
				have = true
				haveVersion = v.version
				need = v.version < vl.versions[0].version
				break
			}
		}

		if need || !have {
			name := globalKeyName(dbi.Key())
			if debug {
				l.Debugf("need repo=%q node=%x name=%q need=%v have=%v haveV=%d globalV=%d", repo, node, name, need, have, haveVersion, vl.versions[0].version)
			}
			fk := nodeKey(repo, vl.versions[0].node, name)
			bs, err := snap.Get(fk, nil)
			if err != nil {
				panic(err)
			}

			var gf scanner.File
			err = gf.UnmarshalXDR(bs[8:])
			if err != nil {
				panic(err)
			}

			if protocol.IsDeleted(gf.Flags) && !have {
				// We don't need deleted files that we don't have
				continue
			}

			if cont := fn(gf); !cont {
				return
			}
		}
	}
}
