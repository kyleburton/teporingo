set -ex
REDIS_PKG="redis-2.2.11.tar.gz"
REDIS_DIR="$(basename "$REDIS_PKG" .tar.gz)"
REDIS_PKG_URL="http://redis.googlecode.com/files/$REDIS_PKG"

test -f "$REDIS_PKG" || wget "$REDIS_PKG_URL"

test -d "$REDIS_DIR" || tar xzvf "$REDIS_PKG"

if [ ! -f "$REDIS_DIR/src/redis-server" ]; then
  cd "$REDIS_DIR"
  make
fi
