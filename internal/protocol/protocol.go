package protocol

type Task struct {
	ID int
	Kind string
	Data []string
	Intermediate map[string]int
	LeaseToken string
}
type Request struct{ WorkerID string }
type Response struct { Done bool; Task Task; HasTask bool }
type CompleteRequest struct {
	WorkerID string
	TaskID int
	Kind string
	LeaseToken string
	Counts map[string]int
}
type CompleteResponse struct{ Accepted bool }
