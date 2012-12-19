#!/usr/bin/env perl

#
# Evaluate a TREC 2012 KBA track run.  The measures computed are
# Precision, Recall,  F1, MAP and nDCG
#
# Version: 1.1.
#
# Usage: ndcg.pl [-v] <qrels> <runfile>\n";
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
my %cum;

main();

sub main(){
  load_qrels();
  load_run();
  pr_eval();
  map_eval();
  means();
}

# load qrel data
sub load_qrels(){
  open QRELS, $qrels_file or die "Can't open `$qrels_file': $!\n";
  while (<QRELS>) {
    chomp;
    next if /^$/;

    my (undef, undef, $did, $query, $score, $rel, $const) = split;
    if($rel > 0){
      $qrel{$query}{$did} = $rel;
    }
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

# evaluate by precision and recall
sub pr_eval(){
  for my $query(sort {$a cmp $b} keys %qrel){
    my $num_ret = scalar keys %{$run{$query}};
    my $num_rel = scalar keys %{$qrel{$query}};

    if(0 == $num_ret){
      $num_ret = 1;
    }
    if(0 == $num_rel){
      $num_rel = 1;
    }

    my $rel_ret = 0;
    for my $did(keys %{$run{$query}}){
      next if !defined $qrel{$query}{$did};
      ++$rel_ret;
    }

    my $precision = $rel_ret / $num_ret;
    my $recall = $rel_ret / $num_rel;
    $cum{$query}{P} = $precision;
    $cum{$query}{R} = $recall;

    printf "$query\tprec\t%6.3f\n", $precision if $verbose;
    printf "$query\trecall\t%6.3f\n", $recall if $verbose;

    $cum{P} += $precision;
    $cum{R} += $recall;
  }
}

# evaluate the results based on MAP
sub map_eval {
  for my $topic(sort {$a<=>$b} keys %qrel){
    my $num_rel = 0;
    my $map = 0;

    my $rank = 1;
    for my $did(sort {$run{$topic}{$b}<=>$run{$topic}{$a}} keys %{$run{$topic}}){
      if(defined $qrel{$topic}{$did}){
        $num_rel ++;
        $map += $num_rel / $rank;
      }
      ++$rank;
    }

    my $rel_total = scalar keys %{$qrel{$topic}};
    if(0 ne $rel_total){
      $map = $map / $rel_total;
    }

    $cum{$topic}{MAP} = $map;
    $cum{MAP} += $map;
  }
}

sub means() {
  if($verbose){
    for my $topic(sort {$a<=>$b} keys %qrel){
      if(!defined $run{$topic}){
        my $map = 0;
        my $p = 0;
        my $r = 0;
        printf "%d\tPrec\t%6.3f\n", $topic, $p;
        printf "%d\tRecal\t%6.3f\n", $topic, $r;
        printf "%d\tMAP\t%6.3f\n", $topic, $map;
        next;
      }
      my $map = $cum{$topic}{MAP};
      my $p = $cum{$topic}{precision};
      my $r = $cum{$topic}{recall};
      printf "%d\tPrec\t%6.3f\n", $topic, $p;
      printf "%d\tRecal\t%6.3f\n", $topic, $r;
      printf "%d\tMAP\t%6.3f\n", $topic, $map;
    }
  }

  my $num_q = scalar keys %qrel;
  print "Topic Number: $num_q\n";
  printf "all\tPrec\t%6.3f\n", $cum{P}/$num_q;
  printf "all\tRecall\t%6.3f\n", $cum{R}/$num_q;
  printf "all\tMAP\t%6.3f\n", $cum{MAP}/$num_q;
}

