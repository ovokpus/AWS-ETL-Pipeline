#!/bin/sh
# Start as root so a bind-mounted lake can be handed to the host uid, then
# drop privileges. LOCAL_UID and LOCAL_GID default to 1000, which matches the
# usual first user on a Linux desktop and this dev environment.
set -eu

lake="${LAKE_DIR:-/data/lake}"
uid="${LOCAL_UID:-1000}"
gid="${LOCAL_GID:-1000}"

mkdir -p "$lake"

if [ "$(id -u)" = "0" ]; then
  chown -R "$uid:$gid" "$lake"
  # Streamlit looks for a writable home. /root is not writable after the drop.
  export HOME=/tmp
  exec setpriv --reuid "$uid" --regid "$gid" --clear-groups -- "$@"
fi

exec "$@"
