// Interface for operations with files via remote protocols sftp, ftp
package producer

import (
	"io"
	"io/fs"
)

type Producer interface {
	Ping(path string) error
	Close() error

	// Open(path string) (io.ReadWriter, error)
	// OpenFile(path string, f int) (io.ReadWriter, error)
	// Create(path string) (io.WriteCloser, error)

	Stat(path string) (fs.FileInfo, error)
	ReadFile(path string) (io.ReadCloser, error)

	// SaveFile writes data to the named file, creating it if necessary.
	// If the file does not exist, SaveFile creates it with permissions perm (before umask);
	// otherwise SaveFile truncates it before writing, without changing permissions.
	// To create an empty file instead of the Reader, pass the nil.
	// Hint: io.Pipe() can be used if an io.Writer is required.
	SaveFile(path string, reader io.ReadCloser) error
	DeleteFile(path string) error

	MakeDir(path string) error
	ReadDir(path string) ([]fs.FileInfo, error)
	DeleteDir(path string) error

	// MkdirAll creates a directory named path, along with any necessary parents,
	// and returns nil, or else returns an error.
	// If path is already a directory, MkdirAll does nothing and returns nil.
	// If path contains a regular file, an error is returned
	MakedirAll(path string) error

	//Rename file or directory
	Rename(oldname, newname string) error

	//Remove removes the named file or empty directory.
	//An error will be returned if no file or directory with the specified path exists, or if the specified directory is not empty.
	//If there is an other error, the error chain will contain fs.ErrInvalid
	Remove(path string) error

	//RemoveAll removes path and any children it contains. It removes everything it can but returns the first error it encounters.
	//If the path does not exist, RemoveAll returns nil (no error). If there is an other error, the error chain maybe contain fs.ErrInvalid
	RemoveAll(path string) error
}
