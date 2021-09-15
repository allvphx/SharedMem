package concurrents

type SharedMemory interface {
	Init(size int, memberN int) bool
	Read(key int) (interface{}, bool)
	Write(key int, value interface{}) bool
	GetMap() (*[]interface{}, bool)
}
