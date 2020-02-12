# Speed-up-search-in-Big-File-with-python
Speed up search in Big File with python

Sono partito da un file molto grande ~250GB
dunque avrebbe richiesto tantissimo tempo per il load utilizzano pandas. 

1 ) ho diviso il file per cromosomi utilizzando bash
```
zcat /lustre/home/enza/CADD/whole_genome_SNVs.tsv.gz | awk -F"\t" 'NR==2{header=$0}NR>1&&!a[$1]++{print header > ("SNV_chr"$1".tsv"
)}NR>1{print > ("SNV_chr"$1".tsv")}'

```
2) ho caricato tutti i file in un solo comando usando dask.dataframe

