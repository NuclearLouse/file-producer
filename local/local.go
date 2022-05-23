package local

import (
	"errors"
	"io"
	"io/fs"
	"os"
)

type localProducer struct{}

func NewProducer() (*localProducer, error) {
	return new(localProducer), nil
}

func (p *localProducer) Ping(_ string) error {
	return nil
}

func (*localProducer) Close() error {
	return nil
}

func (*localProducer) Create(path string) (io.WriteCloser, error) {
	return os.Create(path)
}

func (*localProducer) MakedirAll(path string) error {
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return os.MkdirAll(path, 0666)
	}
	return nil
}

func (*localProducer) ReadFile(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func (p *localProducer) SaveFile(path string, reader io.ReadCloser) error {
	bytes, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	// file, err := os.OpenFile()
	return os.WriteFile(path, bytes, 0777)
}

func (*localProducer) ReadDir(path string) ([]fs.FileInfo, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return dir.Readdir(-1)
}

func (*localProducer) Remove(path string) error {
	return os.Remove(path)
}

func (*localProducer) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (*localProducer) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

func (*localProducer) DeleteFile(path string) error {
	return os.Remove(path)
}

func (*localProducer) MakeDir(path string) error {
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return os.Mkdir(path, 0700)
	}
	return nil
}

func (*localProducer) DeleteDir(path string) error {
	return os.Remove(path)
}

func (p *localProducer) Stat(path string) (fs.FileInfo, error) {
	return os.Stat(path)
}
