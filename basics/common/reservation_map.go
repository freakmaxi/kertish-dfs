package common

type ReservationMap struct {
	Id       string       `json:"reservationId"`
	Clusters []ClusterMap `json:"clusters"`
}
