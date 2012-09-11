#!/usr/bin/env perl

#
# Evaluate a TREC 2012 KBA track run.  The measures computed are
# Precision, Recall and F1 at different levels.
#
# Version: 1.1.
#
# Usage: eval.pl [-v] <qrels> <runfile>\n";
#        -v: verbose mode (default: no)
#        <qrels> and <runfile> may be gzipped.
#

use strict;
use Getopt::Long;

my $usage = "eval.pl [-v] <qrels> <runfile>\n";
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
  score();
  means();
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

    my ($query, $did, $score) = split;
    $run{$query}{$did} = $score;
  }
  close RUN;
}

# do evaluation
sub score {
  for my $query(sort {$a cmp $b} keys %qrel){
    my $num_ret = scalar keys %{$run{$query}};
    my $num_rel = scalar keys %{$qrel{$query}};

    my $rel_ret = 0;
    for my $did(keys %{$run{$query}}){
      next if !defined $qrel{$query}{$did};
      ++$rel_ret;
    }

    $eval{$query}{num_ret} = $num_ret;
    $eval{$query}{num_rel} = $num_rel;
    $eval{$query}{rel_ret} = $rel_ret;
  }
  my $num_query = scalar keys %eval;
  print "$num_query queries in total\n";
}

sub means() {
  my $sum_rel_ret = 0;
  for my $query(sort {$a cmp $b} keys %eval){
    my $num_ret = $eval{$query}{num_ret};
    my $num_rel = $eval{$query}{num_rel};
    my $rel_ret = $eval{$query}{rel_ret};

    if(0 == $num_ret){
      $num_ret = 1;
    }
    if(0 == $num_rel){
      $num_rel = 1;
    }
    
    $sum_rel_ret += $rel_ret;
    my $precision = $rel_ret / $num_ret;
    my $recall = $rel_ret / $num_rel;
    $cum{$query}{precision} = $precision;
    $cum{$query}{recall} = $recall;
    printf "$query\tprec\t%6.3f\n", $precision if $verbose;
    printf "$query\trecl\t%6.3f\n", $recall if $verbose;
  }

  my $sum_precision = 0;
  my $sum_recall = 0;
  for my $query(sort {$a cmp $b} keys %cum){
    $sum_precision += $cum{$query}{precision};
    $sum_recall += $cum{$query}{recall};
  }
  my $num_query = scalar keys %eval;

  my $all_precision = $sum_precision / $num_query;
  my $all_recall = $sum_recall / $num_query;
  printf "all\tsum_rel_ret\t%6.3d\n", $sum_rel_ret;
  printf "all\tprec\t%6.3f\n", $all_precision;
  printf "all\trecall\t%6.3f\n", $all_recall;
}

