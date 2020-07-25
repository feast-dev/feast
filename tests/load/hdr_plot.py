#
# hdr-plot.py v0.2.0 - A simple HdrHistogram plotting script.
# Copyright © 2018 Bruno Bonacci - Distributed under the Apache License v 2.0
#
# usage: hdr-plot.py [-h] [--output OUTPUT] [--title TITLE] [--nobox] files [files ...]
#
# A standalone plotting script for https://github.com/giltene/wrk2 and
#  https://github.com/HdrHistogram/HdrHistogram.
#
# This is just a quick and unsophisticated script to quickly plot the
# HdrHistograms directly from the output of `wkr2` benchmarks.

import argparse
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

regex = re.compile(r'\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)')
filename = re.compile(r'(.*/)?([^.]*)(\.\w+\d+)?')


def parse_percentiles(file):
    lines = [line for line in open(file) if re.match(regex, line)]
    values = [re.findall(regex, line)[0] for line in lines]
    pctles = [(float(v[0]), float(v[1]), int(v[2]), float(v[3])) for v in values]
    percentiles = pd.DataFrame(pctles, columns=['Latency', 'Percentile', 'TotalCount', 'inv-pct'])
    return percentiles


def parse_files(files):
    return [parse_percentiles(file) for file in files]


def info_text(name, data):
    textstr = '%-18s\n------------------\n%-6s = %6.2f ms\n%-6s = %6.2f ms\n%-6s = %6.2f ms\n' % (
        name,
        "min", data['Latency'].min(),
        "median", data[data["Percentile"] == 0.5]["Latency"],
        "max", data['Latency'].max())
    return textstr


def info_box(ax, text):
    props = dict(boxstyle='round', facecolor='lightcyan', alpha=0.5)

    # place a text box in upper left in axes coords
    ax.text(0.05, 0.95, text, transform=ax.transAxes,
            verticalalignment='top', bbox=props, fontname='monospace')


def plot_summarybox(ax, percentiles, labels):
    # add info box to the side
    textstr = '\n'.join([info_text(labels[i], percentiles[i]) for i in range(len(labels))])
    info_box(ax, textstr)


def plot_percentiles(percentiles, labels):
    y_range = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
    x_range = [0.25, 0.5, 0.9, 0.99, 0.999, 0.9999, 0.99999, 0.999999]

    fig, ax = plt.subplots(figsize=(24, 16))
    plt.ylim(0, 50)
    # plot values
    for data in percentiles:
        ax.plot(data['Percentile'], data['Latency'])

    # set axis and legend
    ax.grid()
    ax.set(xlabel='Percentile',
           ylabel='Latency (milliseconds)',
           title='Latency Percentiles (lower is better)')
    ax.set_xscale('logit')
    plt.yticks(y_range)
    plt.xticks(x_range)
    majors = ["25%", "50%", "90%", "99%", "99.9%", "99.99%", "99.999%", "99.9999%"]
    ax.xaxis.set_major_formatter(ticker.FixedFormatter(majors))
    ax.xaxis.set_minor_formatter(ticker.NullFormatter())
    plt.legend(bbox_to_anchor=(0., 1.02, 1., .102),
               loc=3, ncol=2, borderaxespad=0.,
               labels=labels)

    return fig, ax


def arg_parse():
    parser = argparse.ArgumentParser(description='Plot HDRHistogram latencies.')
    parser.add_argument('files', nargs='+', help='list HDR files to plot')
    parser.add_argument('--output', default='latency.png',
                        help='Output file name (default: latency.png)')
    parser.add_argument('--title', default='', help='The plot title.')
    parser.add_argument("--nobox", help="Do not plot summary box",
                        action="store_true")
    args = parser.parse_args()
    return args


def main():
    # print command line arguments
    args = arg_parse()

    # load the data and create the plot
    pct_data = parse_files(args.files)
    labels = [re.findall(filename, file)[0][1] for file in args.files]
    # plotting data
    fig, ax = plot_percentiles(pct_data, labels)
    # plotting summary box
    if not args.nobox:
        plot_summarybox(ax, pct_data, labels)
    # add title
    plt.suptitle(args.title)
    # save image
    plt.savefig(args.output)
    print("Wrote: " + args.output)


if __name__ == "__main__":
    main()
