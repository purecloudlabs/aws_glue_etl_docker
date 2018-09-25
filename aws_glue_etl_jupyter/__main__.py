import json
import os
import argparse
import glob
from pprint import pprint

def cleanWorkbooks(path):
    print('cleaning workbooks ' + path)
    print(glob.glob(path + "/*.ipynb"))
    for workbook in glob.glob(path + "/*.ipynb"):
        input_file = open (workbook)
        notebookContents = json.load(input_file)

        for cell in notebookContents['cells']:
            if 'metadata' in cell:
                cell['metadata'] = {}

            if 'outputs' in cell:
                cell['outputs'] = []

            if 'execution_count' in cell:
                cell['execution_count'] = 0

        with open(workbook, 'w') as out:
            json.dump(notebookContents, out, indent=4, separators=(',', ': '))


def buildWorkbooks(path, outputdir):
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)

    for workbook in glob.glob(path + "/*.ipynb"):
        print(os.path.basename(workbook))

        # with open(workbook) as fp:
        #     for i, line in enumerate(fp):
        #         if "\xe2" in line:
        #             print i, repr(line)

        input_file = open (workbook)
        notebookContents = json.load(input_file)

        out = open('{}/{}'.format(outputdir, workbook.replace(path, "").replace("ipynb","py")), 'w+')

        for cell in notebookContents['cells']:
            if cell['cell_type'] == "code" and len(cell['source']) > 0 and "#LOCALDEV" not in cell['source'][0]:
                for line in cell['source']:
                    if line[0] != '!':
                        out.write(line)
            elif cell['cell_type'] == "markdown":
                
                out.write('\n\'\'\'\n')
                for line in cell['source']:
                    out.write("#" + line)
                out.write('\n\'\'\'\n')


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('command', help='Command to run (clean, build)')
    parser.add_argument('--path', help='Path to workbooks')
    parser.add_argument('--outdir', help='Output path')
    
    args = parser.parse_args()

    if args.command == "clean":
        cleanWorkbooks(args.path)
    elif args.command == "build":
        buildWorkbooks(args.path, args.outdir)
        
if __name__ == '__main__':
    main()
