module pmaas.io/plugins/dblog

go 1.25.1

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/lib/pq v1.10.9
	pmaas.io/spi v0.0.0
	pmaas.io/common v0.0.0
)

replace pmaas.io/common => ../../pmaas-common

replace pmaas.io/spi => ../../pmaas-spi

