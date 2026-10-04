package devtraffic

import "syscall"

// setReceiveBuffer sets a socket's receive buffer, so a slow reader fills
// it, and then the server's send buffer, quickly.
func setReceiveBuffer(c syscall.RawConn, n int) error {
	var serr error
	if err := c.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, n)
	}); err != nil {
		return err
	}
	return serr
}
