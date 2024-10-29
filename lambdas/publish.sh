#!/bin/bash
#

if [ $# -eq 0 ]; then
    echo "erro, pack <lambda>"
    exit 1
fi

echo $(test -f "$i")

if [[ ! -d "$1" ]]; then
   echo "$1 nao encontrado"
   exit 1
fi

json=$(aws --profile dev1 lambda list-functions)
sz=$(printf '%s' "$json" | jq -r '.Functions | length')
#sz=$(cat test.json | jq -r '.Layers | length')

#echo $sz

arn=""


for (( j=0; j<${sz}; j++ ));
do
    item=$(printf '%s' "$json" | jq -r .Functions[$j])

    name=$(printf '%s' "$item" | jq -r .FunctionName)
    echo $name
    if [ "$name" = "$1" ]; then
        arn=$(printf '%s' "$item" | jq -r .FunctionArn)
    fi

done

if [ "$arn" = "" ]; then
    echo 'funcao nao encontrada'
    exit 1
fi

echo "funcao encontrada. arn $arn"


fname="tmp/$1.zip"
echo "criando zip"
rm -rf $fname
#cp sn_http.py python/lib/python3.12/site-packages/
zip -rq $fname $1 
echo "zip criado em $fname"

rm -rf out.tmp

cmd='aws --profile dev1 lambda update-function-code --function-name '"$1"' --zip-file fileb://'"$fname"
echo $cmd
eval $cmd | cat >> out.tmp

