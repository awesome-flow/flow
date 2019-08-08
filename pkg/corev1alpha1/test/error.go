package test

func EqErr(e1, e2 error) bool {
	if e1 == nil || e2 == nil {
		return e1 == e2
	}
	return e1.Error() == e2.Error()
}
