#!/usr/bin/python3
#Create Index CADD
import json
import pandas as pd
import glob
import os,argparse
from glob import iglob  

#################################################
##############  Function wc -l  #################
################################################# 
def get_chunk_line_count(ranges):
	name, start, stop, blocksize = ranges
	left = stop - start

	def blocks(f, left):
		while left > 0:
			b = f.read(min(left, blocksize))
			if b:
				yield b
			else:
				break
			left -= len(b)

	with open(name, 'r') as f:
		f.seek(start)
		return sum(bl.count('\n') for bl in blocks(f, left))

def get_file_offset_ranges(name, blocksize=65536, m=1):
	fsize = os.stat(name).st_size
	import multiprocessing
	chunksize = (fsize // multiprocessing.cpu_count()) * m
	n = fsize // chunksize

	ranges = []
	for i in range(0, n * chunksize, chunksize):
		ranges.append((name, i, i + chunksize, blocksize))
	if fsize % chunksize != 0:
		ranges.append((name, ranges[-1][2], fsize, blocksize))

	return ranges

def wc_proc_pool_exec(name, blocksize=65536):
	import concurrent.futures as futures
	from concurrent.futures import ProcessPoolExecutor
	ranges = get_file_offset_ranges(name, blocksize)

	with ProcessPoolExecutor(max_workers=len(ranges)) as executor:
		results = [executor.submit(get_chunk_line_count, param) for param in ranges]
		return sum([future.result() for future in futures.as_completed(results)])
#################################################

def create_CADDjson(file_list_cadd,chunksize = 100000,progress_bar = "False"):
	
	if progress_bar == "True":
		from tqdm import tqdm 
		for file in file_list_cadd:
			file_base = os.path.basename(file)
			total_file = int(wc_proc_pool_exec(file)/chunksize)
			print("Creation of {y} Jsons for file: {x}".format(y=total_file,x=file_base))
			with tqdm(total=total_file,ncols=80) as pbar:
				for chunk in pd.read_csv(file,sep="\t", chunksize=chunksize):
					pos0 = chunk.iloc[0,1]
					pos1 = chunk.iloc[-1,1]
					chrom = chunk.iloc[0,0]
					chunk["key"] = chunk["#Chrom"].map(str)+":"+chunk["Pos"].map(str)+":/"+chunk["Alt"]
					chunk.set_index("key")["RawScore"].to_json("cadd_chr{chrom}_{pos0}_{pos1}.json".format(chrom=chrom,pos0=pos0,pos1=pos1),orient="index")
					pbar.update(1)
	else:

		try:
			for file in file_list_cadd:
				for chunk in pd.read_csv(file,sep="\t", chunksize=chunksize):
					pos0 = chunk.iloc[0,1]
					pos1 = chunk.iloc[-1,1]
					chrom = chunk.iloc[0,0]
					chunk["key"] = chunk["#Chrom"].map(str)+":"+chunk["Pos"].map(str)+":/"+chunk["Alt"]
					chunk.set_index("key")["RawScore"].to_json("cadd_chr{chrom}_{pos0}_{pos1}.json".format(chrom=chrom,pos0=pos0,pos1=pos1),orient="index")
		except Exception as e:
			print("Error in create_CADDjson fucntion...")
			print("[QUIT]")

def create_file_index(file_list_json,output_name):
	#path_to_folder_jsons = #string-like
	try:
		json_Serie = pd.Series(file_list_json,name="file_name",dtype="object").to_frame()
		json_Serie.file_name = json_Serie.file_name.apply(lambda x: os.path.basename(x))  
		ranges = json_Serie.file_name.str.split("_",expand=True)[[2,3]]
		ranges[3] = ranges[3].str.replace(".json","")
		json_Serie.loc[:,"chr"] = json_Serie.file_name.str.split("_",expand=True)[1]
		json_Serie.loc[:,"lows"] = (ranges[2]).astype(int)
		json_Serie.loc[:,"ups"] = (ranges[3]).astype(int)
		dirname = os.path.dirname((file_list_json[0]))
		json_Serie.to_csv(dirname+"/{output_name}.tsv".format(output_name=output_name),sep="\t",index=False)
	except Exception as e:
		print("Error in Create file index function...")
		print("[QUIT]")
		

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-i", help="path to input file CADD [dirname]",type=str,required=True)
	parser.add_argument("-chunk", help="chunksize of the json files, default = 100000",default=100000,required=False)
	parser.add_argument("-o", help="output_name, default [ index_file_cadd ]",type=str, default="index_file_cadd", required= False)
	parser.add_argument("-skip", help="skip creation json, create only index file",required=False)
	parser.add_argument("-progress", help="print progress bar",default="False",required=False)
	args = parser.parse_args()

	cadd_file = args.i	 
	skip_json = args.skip
	output_name = args.o
	progress_bar = args.progress
	chunksize = args.chunk

	rootdir_glob_cadd = (cadd_file+"/*chr*.tsv").replace("//","/")
	file_list_cadd = [f for f in iglob(rootdir_glob_cadd, recursive=True) if os.path.isfile(f)]

	print("")
	print("Input FILEs :       ",	cadd_file)
	print("output Name :       ",	output_name)
	print("skip_json :         ",	skip_json)
	print("progress bar :      ",   progress_bar)
	print("chunksize :         ",	chunksize)
	print("rootdir_glob_cadd : ",	rootdir_glob_cadd)
	print("file_list_cadd :    ",	file_list_cadd)
	print("")

	if skip_json == None:
		create_CADDjson(file_list_cadd,chunksize,progress_bar)
		rootdir_glob_json = (cadd_file+"/*chr*.json").replace("//","/")
		file_list_json = [f for f in iglob(rootdir_glob_json, recursive=True) if os.path.isfile(f)]
		create_file_index(cadd_file,output_name)
		print("Creation index... [Done]")
	else:
		rootdir_glob_json = (cadd_file+"/*.json").replace("//","/")
		file_list_json = [f for f in iglob(rootdir_glob_json, recursive=True) if os.path.isfile(f)]
		create_file_index(file_list_json,output_name)
		print("Creation index... [Done]")

if __name__ == "__main__":
	main()
