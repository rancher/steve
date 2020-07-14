#kubectl create -f - << EOF
#kind: ConfigMap
#apiVersion: v1
#metadata:
#  name: test
#  ownerReferences:
#  - kind: bad
#    apiVersion: bad
#    name: bad
#    uid: "${RANDOM}${RANDOM}"
#EOF
#
#while true; do
#    kubectl apply -f - << EOF
#kind: ConfigMap
#apiVersion: v1
#metadata:
#  name: test
#  ownerReferences:
#  - kind: bad
#    apiVersion: bad
#    name: bad
#    uid: "${RANDOM}${RANDOM}"
#EOF
#
#done
while true; do
    (kubectl create -f - -o json | kubectl delete -f - ) << EOF
kind: ConfigMap
apiVersion: v1
metadata:
  generateName: test-
  ownerReferences:
  - kind: bad
    apiVersion: bad
    name: bad
    uid: "${RANDOM}${RANDOM}"
EOF
done
