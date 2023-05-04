from pyspark import SparkConf, SparkContext
import sys

def main():
    conf = SparkConf().setAppName("DNACount")
    sc = SparkContext(conf=conf)

    input_file = sys.argv[1]
    dna_rdd = sc.textFile(input_file)

    k = 10  # Number of most common sequences to find
    three_seq_rdd = dna_rdd.flatMap(lambda line: [line[i:i+3] for i in range(len(line) - 2)])
    four_seq_rdd = dna_rdd.flatMap(lambda line: [line[i:i+4] for i in range(len(line) - 3)])

    top_three_seqs = three_seq_rdd.map(lambda seq: (seq, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False).take(k)
    top_four_seqs = four_seq_rdd.map(lambda seq: (seq, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False).take(k)

    print(f"Top {k} 3-sequences:")
    for seq, count in top_three_seqs:
        print(seq, count)

    print(f"Top {k} 4-sequences:")
    for seq, count in top_four_seqs:
        print(seq, count)

    sc.stop()

if __name__ == "__main__":
    main()