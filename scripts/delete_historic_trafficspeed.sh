#!/bin/bash
psql -U postgres -d research -h mimas.geodan.nl  -c "DELETE FROM ndw.trafficspeed WHERE date < NOW() - INTERVAL '4 days'"

