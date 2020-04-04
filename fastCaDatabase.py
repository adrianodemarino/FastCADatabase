#Create Index CADD
import json
import pandas as pd
import glob

def create_CADDjson(path_to_folder_tsv,chunksize = 100000,progress_bar = False):
	filename = glob.glob(path_to_folder_tsv+"*.tsv")
	to_import = list(range(len(filename)))
	if progress_bar == True:
		from tqdm import tqdm 
		with tqdm(total=len(to_import),ncols=80) as pbar:
			for file in filename:
				for chunk in pd.read_csv(file,sep="\t", chunksize=chunksize):
					pos0 = chunk.iloc[0,1]
					pos1 = chunk.iloc[-1,1]
					chrom = chunk.iloc[0,0]
					chunk["key"] = chunk["#Chrom"].map(str)+":"+chunk["Pos"].map(str)+":/"+chunk["Alt"]
					chunk.set_index("key")["RawScore"].to_json("cadd_chr{chrom}_{pos0}_{pos1}.json".format(chrom=chrom,pos0=pos0,pos1=pos1),orient="index")
				pbar.update(1)
	else:
		for file in filename:
			for chunk in pd.read_csv(file,sep="\t", chunksize=chunksize):
				pos0 = chunk.iloc[0,1]
				pos1 = chunk.iloc[-1,1]
				chrom = chunk.iloc[0,0]
				chunk["key"] = chunk["#Chrom"].map(str)+":"+chunk["Pos"].map(str)+":/"+chunk["Alt"]
				chunk.set_index("key")["RawScore"].to_json("cadd_chr{chrom}_{pos0}_{pos1}.json".format(chrom=chrom,pos0=pos0,pos1=pos1),orient="index")
	return 0

def create_file_index(path_to_folder_jsons,output_name):
	#path_to_folder_jsons = #string-like
	print(">>> Creating index file...")
	#create dataframe info / indice
	json_Serie = pd.Series(glob.glob(path_to_folder_jsons+"*.json"),name="file_name").to_frame()
	ranges = json_Serie.file_name.str.split("_",expand=True)[[2,3]]
	ranges[3] = ranges[3].str.replace(".json","")
	json_Serie.loc[:,"chr"] = json_Serie.file_name.str.split("_",expand=True)[1]
	json_Serie.loc[:,"lows"] = (ranges[2]).astype(int)
	json_Serie.loc[:,"ups"] = (ranges[3]).astype(int)
	lows = json_Serie["lows"].values  # the lower bounds
	ups = json_Serie["ups"].values # the upper bounds
	json_Serie.to_csv("{output_name}.tsv".format(output_name=output_name),sep="\t",index=False)
	print("QUIT")
	return 0



with open('cadd_chr9_10001_43334.json') as f: 
	df = json.load(f) 


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", help="path to input file CADD",type=str,required=True)
    parser.add_argument("-skip", help="skip the first part and create only index file",required=False)
    parser.add_argument("-progress", help="print progress bar",required=False)
    parser.add_argument("-o", help="output name, default [ index_file_cadd ]",type=str, default="index_file_cadd", required= False)
    args = parser.parse_args()

    index_file = args.i     
    skip_option = args.skip
    output_name = args.o
    progress_bar = args.progress

    print("")
    print("Input FILEs : ",index_file)
    print("CADD FILEs : ",output_name)
    print("skip_option : ",skip_option)
    print("progress bar : chr",progress_bar)

    