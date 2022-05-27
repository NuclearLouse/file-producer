package sftp

import (
	"errors"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"strings"
	"time"

	gosftp "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// Config expected values:
// AuthMethod : "key", "password", "keyboard".
// The default ftp port:21, ssh and sftp port:22".
type Config struct {
	Host           string        `cfg:"host"`
	Port           string        `cfg:"port"`
	AuthMethod     string        `cfg:"auth_method"`
	User           string        `cfg:"user"`
	Password       string        `cfg:"pass"`
	PrivateKeyFile string        `cfg:"private_key"`
	Timeout        time.Duration `cfg:"timeout"`
}

type sftpProducer struct {
	clientSSH  *ssh.Client
	clientSFTP *gosftp.Client
}

func NewProducer(clientSSH *ssh.Client, clientOptions []gosftp.ClientOption) (*sftpProducer, error) {
	clientSFTP, err := gosftp.NewClient(clientSSH, clientOptions...)
	if err != nil {
		return nil, err
	}
	return &sftpProducer{
		clientSSH:  clientSSH,
		clientSFTP: clientSFTP,
	}, nil
}

func NewClientSSH(c *Config) (*ssh.Client, error) {

	cfg := &ssh.ClientConfig{
		User:            c.User,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         c.Timeout,
	}

	switch c.AuthMethod {
	case "key":
		privateKey, err := ioutil.ReadFile(c.PrivateKeyFile)
		if err != nil {
			return nil, err
		}
		signer, err := ssh.ParsePrivateKey(privateKey)
		if err != nil {
			return nil, err
		}
		cfg.Auth = []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		}
	case "password":
		cfg.Auth = []ssh.AuthMethod{
			ssh.Password(c.Password),
		}
	case "keyboard":
		cfg.Auth = []ssh.AuthMethod{
			ssh.KeyboardInteractive(func(user, instruction string, questions []string, echos []bool) ([]string, error) {
				// Just sends the password back for all questions
				answers := make([]string, len(questions))
				for i := range answers {
					answers[i] = c.Password
				}
				return answers, nil
			}),
		}
	default:
		return nil, errors.New("[" + c.AuthMethod + "] unsupported authentication method")
	}

	if c.Port == "" {
		c.Port = "22"
	}

	return ssh.Dial("tcp", c.Host+":"+c.Port, cfg)
}

func (p *sftpProducer) Ping(path string) error {
	info, err := p.clientSFTP.Stat(path)
	if err != nil {
		return err
	}
	if info == nil {
		return errors.New("unsupported server")
	}
	return nil
}

func (p *sftpProducer) Close() error {
	err := p.clientSFTP.Close()
	err = p.clientSSH.Close()
	return err
}

func (p *sftpProducer) MakedirAll(path string) error {
	return p.clientSFTP.MkdirAll(path)
}

func (p *sftpProducer) ReadFile(path string) (io.ReadCloser, error) {
	return p.clientSFTP.Open(path)
}

//Hint: io.Pipe() can be used if an io.Writer is required.
func (p *sftpProducer) SaveFile(path string, reader io.ReadCloser) error {
	if reader == nil {
		_, err := p.clientSFTP.Create(path)
		return err
	}
	file, err := p.clientSFTP.OpenFile(path, os.O_RDWR|os.O_TRUNC|os.O_CREATE)
	if err != nil {
		return err
	}
	defer file.Close()

	srcBytes, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	dstBytes, err := file.Write(srcBytes)
	if err != nil {
		return err
	}
	if len(srcBytes) != dstBytes {
		return errors.New("data sizes do not match")
	}
	return nil
}

func (p *sftpProducer) ReadDir(path string) ([]fs.FileInfo, error) {
	return p.clientSFTP.ReadDir(path)
}

func (p *sftpProducer) Remove(path string) error {
	err := p.clientSFTP.Remove(path)
	if err != nil && err == fs.ErrPermission {
		return p.clientSFTP.RemoveDirectory(path)
	}
	return err
}

func (p *sftpProducer) Rename(oldname, newname string) error {
	return p.clientSFTP.Rename(oldname, newname)
}

func (p *sftpProducer) DeleteFile(path string) error {
	return p.clientSFTP.Remove(path)
}

func (p *sftpProducer) MakeDir(path string) error {
	return p.clientSFTP.Mkdir(path)
}

func (p *sftpProducer) DeleteDir(path string) error {
	return p.clientSFTP.RemoveDirectory(path)
}

func (p *sftpProducer) RemoveAll(path string) error {
	return p.RemoveAllRecursive(path)
}

func (p *sftpProducer) Stat(path string) (fs.FileInfo, error) {
	return p.clientSFTP.Stat(path)
}

// RemoveAll removes path and any children it contains.
// It removes everything it can but returns the first error
// it encounters. If the path does not exist, RemoveAll
// returns nil (no error).
func (p *sftpProducer) RemoveAllRecursive(path string) error {

	if path == "" {
		// fail silently to retain compatibility with previous behavior
		// of RemoveAll. See issue 28830.
		return nil
	}
	// Simple case: if Remove works, we're done.
	err := p.Remove(path)
	switch {
	case err == nil:
		return nil
	case err != nil:
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if status, ok := err.(*gosftp.StatusError); ok {
			if !strings.Contains(status.Error(), "Directory is not empty") {
				return err
			}
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
				for _, info := range infos {
					names = append(names, info.Name())
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
		}

		if len(names) < entities {
			err1 := p.DeleteDir(path)
			if err1 == nil || (err1 != nil && errors.Is(err1, fs.ErrNotExist)) {
				return nil
			}
			if err != nil {
				return err
			}
		}
	}

	// Remove directory.
	err1 := p.DeleteDir(path)
	if err1 == nil || (err1 != nil && errors.Is(err1, fs.ErrNotExist)) {
		return nil
	}

	return err1
}
