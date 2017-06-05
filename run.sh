# for i in {1..12}; do
#   ./test.lua --thread_nu $i | grep "thread_nu\|AVG"
# done

for i in {12..12}; do
  ./s3/test.py --thread_nu $i | grep "thread_nu\|AVG"
done
