#!/bin/bash
psql -U postgres -d research -h mimas3.geodan.nl  -c "DELETE FROM ndw.trafficspeed WHERE date < NOW() - INTERVAL '4 days'"

