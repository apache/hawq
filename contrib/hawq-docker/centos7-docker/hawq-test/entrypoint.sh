#!/bin/bash

if [ -z "${NAMENODE}" ]; then
  export NAMENODE=${HOSTNAME}
fi

if [ ! -f /etc/profile.d/hadoop.sh ]; then
  echo '#!/bin/bash' | sudo tee /etc/profile.d/hadoop.sh
  echo "export NAMENODE=${NAMENODE}" | sudo tee -a /etc/profile.d/hadoop.sh
  sudo chmod a+x /etc/profile.d/hadoop.sh
fi

sudo start-hdfs.sh
sudo sysctl -p

exec "$@"
