#!/bin/zsh

MSG=""
W=1

while [[ $# -gt 0 ]]; do
	case $1 in
	-m | --message)
		MSG="$2"
		shift 2
		;;
	-w | --write-concern)
		W="$2"
		shift 2
		;;
	*)
		echo "Usage: $0 -m <message> -w <write-concern>"
		exit 1
		;;
	esac
done

echo "sending w=$W request"
START=$(date +%s.%N)
curl -s localhost:5000/messages -H 'Content-Type: application/json' -d "{\"message\":\"$MSG\",\"w\":$W}"
END=$(date +%s.%N)
ELAPSED=$(echo "$END - $START" | bc)
echo "response took ${ELAPSED}s"
echo ""
