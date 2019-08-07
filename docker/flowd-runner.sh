set -e

FLOWD=./builds/flowd
PLUGIN_PATH="/go/src/flowd/plugins/"

build_plugin () {
	plugin="${1}"
	echo "Building plugin ${plugin}"
	cd "${plugin}"
	if test -f go.mod
	then
		go mod verify
	fi
	if test -d Gopkg.toml
	then
		dep ensure
	fi
	go build -buildmode=plugin
	cd ..
}

if test -d ${PLUGIN_PATH}
then
	oldpath=$(pwd)
	cd "${PLUGIN_PATH}"
	for p in $(ls -l .); do
		if test -d $p
		then
			build_plugin "${p}"
		fi
	done
	cd "${oldpath}"
fi

echo "Launching flowd"
dep ensure
${FLOWD} -plugin.path="${PLUGIN_PATH}" $@
