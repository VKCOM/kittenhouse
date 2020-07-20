package srvfunc

/*
#include <unistd.h>
*/
import "C"

// ChangeUser установка ID пользователя для текущего потока
func ChangeUser(user string) error {
	if uid, err := LookupUidByName(user); err != nil {
		return err
	} else if status := C.setuid(C.uid_t(uid)); status != 0 {
		return EWrap(ErrSyscallFail)
	}

	return nil
}

// ChangeGroup устанавливает ID группы для текущего потока
func ChangeGroup(group string) error {
	if gid, err := LookupGidByName(group); err != nil {
		return err
	} else if status := C.setgid(C.gid_t(gid)); status != 0 {
		return EWrap(ErrSyscallFail)
	}

	return nil
}
