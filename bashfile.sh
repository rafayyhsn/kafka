#!/bin/bash

# Open a terminal window for Zookeeper
gnome-terminal -- bash -c 'cd kafka/bin && ./zookeeper-server-start.sh ../../config/zookeeper.properties; exec bash'

# Open a terminal window for Kafka server
gnome-terminal -- bash -c 'cd kafka/bin && ./kafka-server-start.sh ../../config/server.properties; exec bash'

# Open a terminal window for consumer.py
gnome-terminal -- bash -c 'cd ~/Desktop/A4 && python consumer.py; exec bash'

# Open a terminal window for consumer2.py
gnome-terminal -- bash -c 'cd ~/Desktop/A4 && python consumer2.py; exec bash'

# Open a terminal window for consumer3.py
gnome-terminal -- bash -c 'cd ~/Desktop/A4 && python consumer3.py; exec bash'

# Open a terminal window for producer.py
gnome-terminal -- bash -c 'cd ~/Desktop/A4 && python producer.py; exec bash'
