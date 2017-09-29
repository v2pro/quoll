package evtstore

import (
	"os"
	"github.com/blang/vfs"
	"path"
	"time"
)

var fs vfs.Filesystem = vfs.OS()
var mockedNow *time.Time

type Store struct {
	RootDir string
}

func (store *Store) Add(eventJson []byte) error {
	ts := now().Format("200601021504")
	file, err := fs.OpenFile(
		path.Join(store.RootDir, ts), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	file.Write(eventJson)
	return nil
}

func now() time.Time {
	if mockedNow != nil {
		return *mockedNow
	}
	return time.Now()
}
