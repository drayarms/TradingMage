#!/usr/bin/env bash
set -euxo pipefail

apt-get update -y
apt-get install -y docker.io ca-certificates curl jq

systemctl enable docker
systemctl start docker

mkdir -p /data/elasticsearch

if ! blkid ${ebs_device}; then
	mkfs.ext4 ${ebs_device}
fi

if ! grep -q "/data/elasticsearch" /etc/fstab; then
	echo "${ebs_device} /data/elasticsearch ext4 defaults,nofail 0 2" >> /etc/fstab
fi

mount -a
chown -R 1000:1000 /data/elasticsearch

docker network create elk-net || true

docker rm -f elasticsearch kibana || true

#-e ES_JAVA_OPTS="-Xms1g -Xmx1g" \
docker run -d \
	--name elasticsearch \
	--restart unless-stopped \
	--network elk-net \
	-p 9200:9200 \
	-e discovery.type=single-node \
	-e xpack.security.enabled=false \
	-e ES_JAVA_OPTS="-Xms512m -Xmx512m" \
	-v /data/elasticsearch:/usr/share/elasticsearch/data \
	docker.elastic.co/elasticsearch/elasticsearch:8.19.0

sleep 45

docker run -d \
	--name kibana \
	--restart unless-stopped \
	--network elk-net \
	-p 5601:5601 \
	-e ELASTICSEARCH_HOSTS=http://elasticsearch:9200 \
	docker.elastic.co/kibana/kibana:8.19.0
