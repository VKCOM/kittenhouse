package srvfunc

/*
#include <unistd.h>
*/
import "C"
import "os"

// ChangeUser установка ID пользователя для текущего потока
func ChangeUser(user string) error {
	if uid, err := LookupUidByName(user); err != nil {
		return err
	} else if status := C.setuid(C.__uid_t(uid)); status != 0 {
		return EWrap(ErrSyscallFail)
	}

	// чтобы последующий код, например RPCConn.Connect, видел правильное имя пользователя
	os.Setenv(`USER`, user)

	return nil
}

// ChangeGroup устанавливает ID группы для текущего потока
func ChangeGroup(group string) error {
	if gid, err := LookupGidByName(group); err != nil {
		return err
	} else if status := C.setgid(C.__gid_t(gid)); status != 0 {
		return EWrap(ErrSyscallFail)
	}

	return nil
}
