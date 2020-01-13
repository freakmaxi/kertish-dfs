package common

type Reservation struct {
	Id       string       `json:"reservationId"`
	Clusters []ClusterMap `json:"clusters"`
}
