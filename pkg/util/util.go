package util

type Failer func() error

func ExecEnsure(failers ...Failer) error {
	for _, failer := range failers {
		if err := failer(); err != nil {
			return err
		}
	}
	return nil
}
