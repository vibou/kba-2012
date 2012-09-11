#!/usr/bin/env perl

#
# Analyze the results of wiki-match
#
# Version: 1.1.
#
# Usage: wiki-match-analyze.pl [-v] <qrels> <runfile>\n";
#        -v: verbose mode (default: no)
#        <qrels> and <runfile> may be gzipped.
#

use strict;
use Getopt::Long;

my $usage = "wiki-match-analyze.pl [-v] <qrels> <runfile>\n";
my $verbose = 0;
GetOptions('verbose!' => \$verbose,
    ) or die $usage;

# allow qrels and runfiles to be compressed with gzip
@ARGV = map { /.gz$/ ? "gzip -dc $_ |" : $_ } @ARGV;

my $qrels_file = shift or die $usage;
my $run_file = shift or die $usage;

my %qrel;
my %run;
my %eval;
my %cum;

main();

sub main(){
  load_qrels();
  load_run();
  analyze();
}

# load qrel data
sub load_qrels(){
  open QRELS, $qrels_file or die "Can't open `$qrels_file': $!\n";
  while (<QRELS>) {
    chomp;
    next if /^$/;

    my ($query, $did, $score, $judge1, $judge2) = split;
    $qrel{$query}{$did} = 1;
  }
  close QRELS;
}

# load the data of runfile
sub load_run(){
  open RUN, $run_file or die "Can't open `$run_file': $!\n";
  while (<RUN>) {
    chomp;
    next if /^$/;

    s/\-(\d+) $/\- $1/g;
    my ($lead, $query, $did, $score) = split / - /;
    $run{$query}{$did} = $score;
  }
  close RUN;
}

# do analysis
sub analyze(){
  my $rel_sum = 0;
  my $rel_num = 0;
  my $irrel_sum = 0;
  my $irrel_num = 0;

  for my $query(sort {$a cmp $b} keys %qrel){
    for my $did(keys %{$run{$query}}){
      my $score = $run{$query}{$did};
      if(defined $qrel{$query}{$did}){
        ++ $rel_num;
        $rel_sum += $score;
      }else{
        ++ $irrel_num;
        $irrel_sum += $score;
      }
    }
  }

  # smoothing
  if(0 == $rel_num){
    $rel_num = 1;
  }
  if(0 == $irrel_num){
    $irrel_num = 1;
  }

  # average
  my $rel_avg = $rel_sum / $rel_num;
  my $irrel_avg = $irrel_sum / $irrel_num;

  # standard deviation
  my $rel_sq_sum = 0;
  my $irrel_sq_sum = 0;
  for my $query(sort {$a cmp $b} keys %qrel){
    for my $did(keys %{$run{$query}}){
      my $score = $run{$query}{$did};
      if(defined $qrel{$query}{$did}){
        my $sq = ($score - $rel_avg)*($score - $rel_avg);
        $rel_sq_sum += $sq;
      }else{
        my $sq = ($score - $irrel_avg)*($score - $irrel_avg);
        $irrel_sq_sum += $sq;
      }
    }
  }
  my $rel_dev = $rel_sq_sum / $rel_num;
  $rel_dev = sqrt($rel_dev);
  my $irrel_dev = $irrel_sq_sum / $irrel_num;
  $irrel_dev = sqrt($irrel_dev);

  printf "rel_num:\t%d\n", $rel_num;
  printf "rel_avg:\t%.3f\n", $rel_avg;
  printf "rel_dev:\t%.3f\n", $rel_dev;
  printf "irrel_num:\t%d\n", $irrel_num;
  printf "irrel_avg:\t%.3f\n", $irrel_avg;
  printf "irrel_dev:\t%.3f\n", $irrel_dev;
}

