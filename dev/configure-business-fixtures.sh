#!/usr/bin/env bash
# Invoke with bash; creates explicit LOCAL business mappings for real authenticated UUIDs.
set -euo pipefail
owner=${1:?Usage: bash dev/configure-business-fixtures.sh AUTHOR_UUID FOREIGN_AUTHOR_UUID}
foreign=${2:?Supply a different authenticated user UUID for foreign-owned media}
base=${SD_STUB_BASE_URL:-http://localhost:8090}
uuid='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
[[ $owner =~ $uuid && $foreign =~ $uuid && $owner != "$foreign" ]] || { echo 'Two distinct UUIDs required' >&2; exit 2; }
# Stable IDs identify only these operator-installed mappings; shared stubs are never reset.
install_media() {
  local suffix=$1 status=$2 actor=$3 asset="019a0000-0000-7000-8000-0000000000${1}" mapping="019a1000-0000-7000-8000-0000000000${1}"
  curl --fail --silent --show-error -X DELETE "$base/__admin/mappings/$mapping" >/dev/null
  curl --fail --silent --show-error -X POST "$base/__admin/mappings" -H 'Content-Type: application/json' --data-binary "{\"id\":\"$mapping\",\"name\":\"operator-media-$suffix\",\"priority\":1,\"request\":{\"method\":\"GET\",\"urlPath\":\"/media/v1/assets/$asset\"},\"response\":{\"status\":200,\"headers\":{\"Content-Type\":\"application/json\"},\"jsonBody\":{\"id\":\"$asset\",\"ownerId\":\"$actor\",\"status\":\"$status\",\"revision\":1}}}" >/dev/null
  printf '%s %s\n' "$asset" "$status"
}
install_media 21 CLEAN "$owner"
install_media 22 QUARANTINED "$owner"
install_media 23 PROCESSING "$owner"
install_media 24 REJECTED "$owner"
install_media 25 CLEAN "$foreign"
mapping=019a1000-0000-7000-8000-000000000031
curl --fail --silent --show-error -X DELETE "$base/__admin/mappings/$mapping" >/dev/null
curl --fail --silent --show-error -X POST "$base/__admin/mappings" -H 'Content-Type: application/json' --data-binary "{\"id\":\"$mapping\",\"name\":\"operator-organization-verified\",\"priority\":1,\"request\":{\"method\":\"GET\",\"urlPath\":\"/organizations/v1/authorizations\",\"queryParameters\":{\"userId\":{\"equalTo\":\"$owner\"},\"placeId\":{\"equalTo\":\"019a0000-0000-7000-8000-000000000001\"},\"action\":{\"equalTo\":\"PROPOSE_CHANGE\"}}},\"response\":{\"status\":200,\"headers\":{\"Content-Type\":\"application/json\"},\"jsonBody\":{\"allowed\":true,\"verified\":true}}}" >/dev/null
printf 'Verified organization fixture installed for the supplied author; no auth roles changed.\n'
