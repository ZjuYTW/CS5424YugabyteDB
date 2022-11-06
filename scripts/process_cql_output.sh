#!/bin/bash
cd ./data/output/measure_log
all_outputs=$(ls -v cql_*)
output_file="clients.csv"
throughput_file="throughput.csv"
txn_throughput_file="txn_throughput.csv"

[ -e ${output_file} ] && rm ${output_file}
[ -e ${throughput_file} ] && rm ${throughput_file}
[ -e ${txn_throughput_file} ] && rm ${txn_throughput_file}

for cql_out in $all_outputs;do
  echo "Processing ${cql_out}, output file: ${output_file}"
  cat ${cql_out} | grep -e '^Total number of' | awk 'BEGIN{ORS=""}{print $6}' >> $output_file
  echo -n "," >> $output_file
  cat ${cql_out} | grep '^Total elapsed time' | awk 'BEGIN{ORS=""}{print $8}' >> $output_file
  echo -n "," >> $output_file
  cat ${cql_out} | grep '^Transaction throughput:' | awk 'BEGIN{ORS=""}{print $3}' >> $output_file
  echo -n "," >> $output_file
  cat ${cql_out} | grep '^Average transaction latency' | awk 'BEGIN{ORS=""}{print $4}' >> $output_file
  echo -n "," >> $output_file
  cat ${cql_out} | grep '^Median transaction latency:' | awk 'BEGIN{ORS=""}{print $4}' >> $output_file
  echo -n "," >> $output_file
  cat ${cql_out} | grep '^95th percentile transaction latency: ' | awk 'BEGIN{ORS=""}{print $5}' >> $output_file
  echo -n "," >> $output_file
  cat ${cql_out} | grep '^99th percentile transaction latency: ' | awk '{print $5}' >> $output_file
  echo "process ${output_file} done"
done

echo "Process throughput..."
MAX=`awk -F ',' '{print $3}' ${output_file}|sort -nr|head -1`
echo "max: ${MAX}"
MIN=`awk -F ',' '{print $3}' ${output_file}|sort -nr|tail -1`
echo "min: ${MIN}"
AVG=`awk -F ',' '{sum += $3}END{print sum/20}' ${output_file}`
echo "avg: ${AVG}"
echo "Overall: ${MIN},${MAX},${AVG}" >> ${throughput_file}

# winston.yan
echo "Processing each transactions..."
declare -a txnNames=("delivery" "new_order" "order_status" "payment" "popular_item" "related_customer" "stock_level" "top_balance")

for tname in ${txnNames[@]}; do
  output_txn_file="${tname}_clients.csv"
  [ -e ${output_txn_file} ] && rm ${output_txn_file}

  for cql_out in $all_outputs;do
    echo "Processing ${cql_out}, output file: ${output_txn_file}"
    cat ${cql_out} | grep -e "^>>> Total number of ${tname}" | awk 'BEGIN{ORS=""}{print $7}' >> $output_txn_file
    echo -n "," >> $output_txn_file
    cat ${cql_out} | grep "^${tname}'s throughput" | awk 'BEGIN{ORS=""}{print $3}' >> $output_txn_file
    echo -n "," >> $output_txn_file
    cat ${cql_out} | grep "^${tname}'s average transaction latency" | awk 'BEGIN{ORS=""}{print $5}' >> $output_txn_file
    echo -n "," >> $output_txn_file
    cat ${cql_out} | grep "^${tname}'s Median transaction latency" | awk 'BEGIN{ORS=""}{print $5}' >> $output_txn_file
    echo -n "," >> $output_txn_file
    cat ${cql_out} | grep "^${tname}'s 99th percentile transaction" | awk '{print $6}' >> $output_txn_file
    echo "process ${output_txn_file} done"
  done

  echo "Process ${tname} throughput..."
  MAX=`awk -F ',' '{print $2}' ${output_txn_file}|sort -nr|head -1`
  echo "max: ${MAX}"
  MIN=`awk -F ',' '{print $2}' ${output_txn_file}|sort -nr|tail -1`
  echo "min: ${MIN}"
  AVG=`awk -F ',' '{sum += $2}END{print sum/20}' ${output_txn_file}`
  echo "avg: ${AVG}"
  echo "${tname}: ${MIN},${MAX},${AVG}" >> ${txn_throughput_file}

done