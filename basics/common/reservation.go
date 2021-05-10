package common

// Reservation struct holds the reservation data for the file uploading
type Reservation struct {
	Id   string `json:"reservationId"`
	Size uint64 `json:"size"`
}
