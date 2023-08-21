import argparse
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def parse_coords_file(file_path):
    coords = []
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.split('%')
            coords.append(tuple(map(float, parts[0].strip()[1:-1].split(','))))
    return coords

def generate_graph(coords, title, x_label, y_label, outfile):
    # Insert (0, 0) at the beginning of the coordinate list
    coords.insert(0, (0, 0))
    coords = list(map(lambda x: (x[0]+1, x[1]), coords))

    df = pd.DataFrame(coords, columns=['x', 'y'])

    csfont = {'fontname': 'Times New Roman'}
    plt.rcParams['font.family'] = "Times New Roman"
    fig, ax = plt.subplots(1)
    fig.set_figheight(12)
    fig.set_figwidth(15)
#     fig.tight_layout(pad=10)
    ax.set_ylim([0, 100])
    ax.set_yticks(list(range(0, 100, 5)), minor=True)
    ax.spines["top"].set_linewidth(3)
    ax.spines["left"].set_linewidth(3)
    ax.spines["right"].set_linewidth(3)
    ax.spines["bottom"].set_linewidth(3)


    ax.tick_params(which='minor', length=9, width=2)
    ax.tick_params(axis='x', labelsize=30, length=14, width=3, pad=8)
    ax.tick_params(axis='y', labelsize=30, length=14, width=3, pad=8)

#     ax.grid(color='k', linestyle='-', linewidth=0.1)
    plt.grid(True, which='both')
    plt.xscale("log")
    sns.set_style('whitegrid')
    sns.lineplot(x='x', y='y', data=df, linewidth=7)
    plt.title(title, fontsize=40, pad=25, **csfont)
    plt.xlabel(x_label, fontsize=30)
    plt.ylabel(y_label, fontsize=30)
    plt.savefig(outfile, dpi=200)

def main():
    parser = argparse.ArgumentParser(description='Generate a graph from coordinates')
    parser.add_argument('--coords-file', required=True, help='Path to the coordinates file')
    parser.add_argument('--outfile', required=True, help='Path to the output file (PNG format)')
    parser.add_argument('--title', required=True, help='Title for the graph')
    parser.add_argument('--x-label', required=True, help='X-axis label')
    parser.add_argument('--y-label', required=True, help='Y-axis label')
    args = parser.parse_args()

    coords = parse_coords_file(args.coords_file)
    generate_graph(coords, args.title, args.x_label, args.y_label, args.outfile)

if __name__ == '__main__':
    main()
