package ftp

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"io"
	"io/fs"
	"os"
	"strings"
	"syscall"

	"github.com/secsy/goftp"
)

type producerFTP struct {
	c *goftp.Client
}

// Config expected values:
// The default ftp port:21.
type Config struct {
	Host       string        `cfg:"host"`
	Port       string        `cfg:"port"`
	User       string        `cfg:"user"`
	Password   string        `cfg:"pass"`
	Timeout    time.Duration `cfg:"timeout"`
	DebugLoger io.Writer
}

func NewProducer(client *goftp.Client) (*producerFTP, error) {
	return &producerFTP{client}, nil
}

// NewClient creates an FTP client using the given config. "hosts" is a list of IP addresses or hostnames
// with an optional port (defaults to 21). Hostnames will be expanded to all the IP addresses they resolve to.
// The client's connection pool will pick from all the addresses in a round-robin fashion.
// If you specify multiple hosts, they should be identical mirrors of each other.
func NewClient(c *Config, hosts ...string) (*goftp.Client, error) {
	cfg := goftp.Config{
		User:     c.User,
		Password: c.Password,
		Timeout:  c.Timeout,
		Logger:   c.DebugLoger,
		// TLSMode: 2,

	}
	if len(hosts) == 0 {
		return goftp.DialConfig(cfg, "127.0.0.1")
	}
	return goftp.DialConfig(cfg, hosts...)
}

func (p *producerFTP) Ping(_ string) error {
	rawConn, err := p.c.OpenRawConn()
	if err != nil {
		return err
	}
	defer rawConn.Close()

	code, msg, err := rawConn.SendCommand("FEAT")
	if err != nil {
		return err
	}
	if code != 211 || !strings.Contains(msg, "REST") {
		return fmt.Errorf("%d :%s: %w", code, msg, fmt.Errorf("unsupported server"))
	}
	return nil
}

func (p *producerFTP) Close() error {
	return p.c.Close()
}

func (p *producerFTP) Stat(path string) (fs.FileInfo, error) {
	return p.c.Stat(path)
}

func (p *producerFTP) ReadFile(path string) (io.ReadCloser, error) {

	pipeReader, pipeWriter := io.Pipe()

	var err error
	go func() {
		err = func() error {
			defer pipeWriter.Close()
			if err := p.c.Retrieve(path, pipeWriter); err != nil {
				return err
			}
			return nil
		}()
	}()

	return pipeReader, err
}

func (p *producerFTP) SaveFile(path string, reader io.ReadCloser) error {
	if reader == nil {
		reader = io.NopCloser(bytes.NewReader([]byte{}))
	}
	return p.c.Store(path, reader)
}

func (p *producerFTP) ReadDir(path string) ([]fs.FileInfo, error) {
	return p.c.ReadDir(path)
}

func (p *producerFTP) Remove(path string) error {
	return p.RemoveAny(path)
}

func (p *producerFTP) RemoveAll(path string) error {
	return p.RemoveAllRecursive(path)
}

func (p *producerFTP) Rename(oldname, newname string) error {
	return p.c.Rename(oldname, newname)
}

func (p *producerFTP) DeleteFile(path string) error {
	return p.c.Delete(path)
}

func (p *producerFTP) MakeDir(path string) error {
	_, err := p.c.Mkdir(path)
	return err
}

func (p *producerFTP) DeleteDir(path string) error {
	return p.c.Rmdir(path)
}

func (p *producerFTP) MakedirAll(path string) error {
	return p.MkdirAll(path)
}

// MkdirAll creates a directory named path, along with any necessary parents,
// and returns nil, or else returns an error.
// If path is already a directory, MkdirAll does nothing and returns nil.
// If path contains a regular file, an error is returned
func (p *producerFTP) MkdirAll(path string) error {
	// Most of this code mimics https://golang.org/src/os/path.go?s=514:561#L13
	// Fast path: if we can tell whether path is a directory or file, stop with success or error.
	dir, err := p.Stat(path)
	if err == nil {
		if dir.IsDir() {
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
	}

	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(path)
	for i > 0 && path[i-1] == '/' { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && path[j-1] != '/' { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent
		err = p.MkdirAll(path[0 : j-1])
		if err != nil {
			return err
		}
	}

	// Parent now exists; invoke Mkdir and use its result.
	if err = p.MakeDir(path); err != nil {
		// Handle arguments like "foo/." by
		// double-checking that directory doesn't exist.
		dir, err1 := p.Stat(path)
		// dir, err1 := c.Lstat(path)
		if err1 == nil && dir.IsDir() {
			return nil
		}
		return err
	}

	return nil
}

// Remove removes the specified file or directory. An error will be returned if no
// file or directory with the specified path exists, or if the specified directory
// is not empty.
func (p *producerFTP) RemoveAny(path string) error {

	dir, err := p.Stat(path)
	if err != nil {
		return fmt.Errorf("%s: %s: %w", path, err.Error(), fs.ErrNotExist)
	}
	if dir.IsDir() {
		if err := p.DeleteDir(path); err != nil {
			return fmt.Errorf("%s: %s: Directory is not empty: %w", path, err.Error(), fs.ErrPermission)

		}
		return nil
	}
	if err := p.DeleteFile(path); err != nil {
		return fmt.Errorf("%s: %s: %w", path, err.Error(), fs.ErrInvalid)
	}
	return nil
}

// RemoveAll removes path and any children it contains.
// It removes everything it can but returns the first error
// it encounters. If the path does not exist, RemoveAll
// returns nil (no error).
func (p *producerFTP) RemoveAllRecursive(path string) error {

	if path == "" {
		// fail silently to retain compatibility with previous behavior
		// of RemoveAll. See issue 28830.
		return nil
	}

	// Simple case: if RemoveAny works, we're done.
	err := p.RemoveAny(path)
	switch {
	case err == nil:
		return nil
	case err != nil:
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if !strings.Contains(err.Error(), "Directory is not empty") {
			return err
		}
	}

DIR:
	for {
		infos, err := p.ReadDir(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}

		entities := len(infos)
		if entities == 0 {
			break
		}
		var names []string
		for {
			numErr := 0
			names := func() []string {
				for _, f := range infos {
					names = append(names, f.Name())
				}
				return names
			}()

			if len(names) == 0 {
				break DIR
			}

			for _, name := range names {
				err1 := p.RemoveAllRecursive(path + "/" + name)
				if err == nil {
					err = err1
				}
				if err1 != nil {
					numErr++
				}
			}
			// If we can delete any entry, break to start new iteration.
			// Otherwise, we discard current names, get next entries and try deleting them.
			if numErr != entities {
				break
			}

			if len(names) == 0 {
				break
			}
			if len(names) < entities {
				err1 := p.RemoveAny(path)
				if err1 == nil || (err1 != nil && errors.Is(err1, fs.ErrNotExist)) {
					return nil
				}
				if err != nil {
					return err
				}
			}
		}
	}
	// Remove directory.
	err1 := p.RemoveAny(path)
	if err1 == nil || (err1 != nil && errors.Is(err1, fs.ErrNotExist)) {
		return nil
	}

	return err1
}
