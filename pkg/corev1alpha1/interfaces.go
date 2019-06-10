package corev1alpha1

type Namer interface {
	Name() string
}

type Receiver interface {
	Receive(*Message) error
}

type Connector interface {
	Connect(nthreads int, receiver Receiver) error
}

type Runner interface {
	Start() error
	Stop() error
}

type Actor interface {
	Connector
	Namer
	Receiver
	Runner
}
