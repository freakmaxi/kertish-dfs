package common

// ReservationMap struct is to hold and export/serialize the file creation
// reservation details that will happen across the dfs farm
type ReservationMap struct {
	Id       string       `json:"reservationId"`
	Clusters []ClusterMap `json:"clusters"`
}
