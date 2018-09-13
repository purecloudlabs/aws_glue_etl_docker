import json
import os
import argparse
import glob

def cleanWorkbooks(path):
    print('cleaning workbooks ' + path)
    print(glob.glob(path + "/*.ipynb"))
    for workbook in glob.glob(path + "/*.ipynb"):
        print(workbook)
        input_file = open (workbook)
        notebookContents = json.load(input_file)
        print(notebookContents)

def buildWorkbooks(path, outputdir):
    for workbook in glob.glob(path + "/*.ipynb"):
        print(os.path.basename(workbook))

        input_file = open (workbook)
        notebookContents = json.load(input_file)

        out = open('{}/{}.py'.format(outputdir, workbook.replace("ipynb","py")), 'w')

        for cell in notebookContents['cells']:
            if cell['cell_type'] == "code" and len(cell['source']) > 0 and "#LOCALDEV" not in cell['source'][0]:
                for line in cell['source']:
                    out.write(line)



def main(args):
    if args.command == "clean":
        cleanWorkbooks(args.path)
    elif args.command == "build":
        buildWorkbooks(args.path, arga.outdir)
    # dir_path = os.path.dirname(os.path.realpath(__file__))

    # notebooks = ['PublicRequestsLocalRegion', 'PublicRequestsNonBookmarked']

    # for notebook in notebooks:
    #     input_file = open ('{}.ipynb'.format(notebook))
    #     notebookContents = json.load(input_file)


    #     out = open('{}/../src/main/resources/{}.py'.format(dir_path, notebook), 'w')

    #     for cell in notebookContents['cells']:
    #         if cell['cell_type'] == "code" and len(cell['source']) > 0 and "#LOCALDEV" not in cell['source'][0]:
    #             for line in cell['source']:
    #                 out.write(line)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()

	# Required arguments
	parser.add_argument('command', help='Command to run (clean, build)')
	parser.add_argument('--path', help='Path to workbooks')
    parser.add_argument('--outdir', help='Output Path')
	args = parser.parse_args()
	main(args)
