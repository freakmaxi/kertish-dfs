package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReservations_CleanUp(t *testing.T) {
	reservations := NewReservations()
	reservations["x"] = &Reservation{Size: 0, ExpiresAt: time.Now().UTC().Add(time.Hour * -25)}

	reservations.CleanUp()

	_, has := reservations["x"]
	assert.False(t, has)
}

func TestCluster_StateString(t *testing.T) {
	cluster := NewCluster("test")
	cluster.State = StateOnline
	cluster.Paralyzed = false
	assert.Equal(t, "Online", cluster.StateString())

	cluster.State = StateReadonly
	cluster.Paralyzed = false
	assert.Equal(t, "Online (RO)", cluster.StateString())

	cluster.State = StateOffline
	cluster.Paralyzed = false
	assert.Equal(t, "Offline", cluster.StateString())

	cluster.State = StateOnline
	cluster.Paralyzed = true
	assert.Equal(t, "Paralyzed", cluster.StateString())

	cluster.State = StateReadonly
	cluster.Paralyzed = true
	assert.Equal(t, "Paralyzed", cluster.StateString())

	cluster.State = StateOffline
	cluster.Paralyzed = true
	assert.Equal(t, "Offline", cluster.StateString())
}
