#!/usr/bin/env perl

#
# Evaluate a TREC 2012 KBA track run.  The measures computed are
# Precision, Recall,  F1, MAP and nDCG@R
#
# Version: 1.1.
#
# Usage: all.pl [-v] <train-or-test> <qrels> <runfile>\n";
#        -v: verbose mode (default: no)
#        <qrels> and <runfile> may be gzipped.
#

use strict;
use Getopt::Long;

my $usage = "all.pl [-v] <train-or-test> <qrels> <runfile>\n";
my $verbose = 0;
GetOptions('verbose!' => \$verbose,
    ) or die $usage;

# allow qrels and runfiles to be compressed with gzip
@ARGV = map { /.gz$/ ? "gzip -dc $_ |" : $_ } @ARGV;

my $train_or_test = shift or die $usage;
my $qrels_file = shift or die $usage;
my $run_file = shift or die $usage;

my %qrel;
my %run;
my %cum;
my $total_rel = 0;
my $total_rel_ret = 0;

unless('train' eq $train_or_test or 'test' eq $train_or_test){
  die "\$train_or_test must be 'train' or 'equal'\!";
}

main();

sub main(){
  load_qrels();
  load_run();
  pr_eval();
  map_eval();
  ndcg_eval();
  means();
}

# load qrel data
sub load_qrels(){
  my $total_rel = 0;
  open QRELS, $qrels_file or die "Can't open `$qrels_file': $!\n";
  while (<QRELS>) {
    chomp;
    next if /^$/;

    my (undef, undef, $did, $query, $score, $rel, $const) = split;
    my ($epoch, undef) = split /-/, $did;
    # this is the epoch time for the last second of 2011, i.e. Dec 31 2011
    # 23:59:59 GMT+0000
    if('train' eq $train_or_test){
      next if $epoch > 1325375999;
    }else{
      next if $epoch <= 1325375999;
    }
    if($score > 0 and $rel > 0){
      $qrel{$query}{$did} = $rel;
      ++$total_rel;
    }
  }
  close QRELS;

  #print "Total rel: $total_rel\n";
  #die;
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

# evaluate by precision, recall and F1
sub pr_eval(){
  for my $query(sort {$a cmp $b} keys %qrel){
    my $num_ret = scalar keys %{$run{$query}};
    my $num_rel = scalar keys %{$qrel{$query}};
    $total_rel += $num_rel;

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
    $total_rel_ret += $rel_ret;
    my $f1 = 0;
    if(0 < $precision + $recall){
      $f1 = 2 * $precision * $recall / ($precision + $recall);
    }
    $cum{$query}{precision} = $precision;
    $cum{$query}{recall} = $recall;
    $cum{$query}{F1} = $f1;

    #printf "$query\tprec\t%6.3f\n", $precision if $verbose;
    #printf "$query\trecall\t%6.3f\n", $recall if $verbose;

    $cum{P} += $precision;
    $cum{R} += $recall;
    $cum{F1} += $f1;
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

# evaluate the results based on nDCG@R
sub ndcg_eval {
  my %ie_qrel;

  for my $topic(sort {$a<=>$b} keys %qrel){
    my $num_rel = 0;
    my $ndcg = 0;

    # prepare for the ideal ranking list based on the judgment file
    my $rank = 1;
    for my $did(sort {$qrel{$topic}{$b}<=>$qrel{$topic}{$a}} 
      keys %{$qrel{$topic}}){
      $ie_qrel{$topic}{$rank} = $qrel{$topic}{$did};
      ++$rank;
    }
    my $num_rel = $rank -1;
   
    $rank = 1;
    my $dcg = 0;
    my $idcg = 0;
    for my $did(sort {$run{$topic}{$b}<=>$run{$topic}{$a}} keys %{$run{$topic}}){
      if(defined $qrel{$topic}{$did}){
        my $e = $qrel{$topic}{$did};

        next if !defined $ie_qrel{$topic}{$rank};
        my $ie = $ie_qrel{$topic}{$rank};
        next if 0 == $ie;

        $dcg += ($e / (log($rank + 1)/log(2)));
        $idcg += ($ie / (log($rank + 1)/log(2)));
      }
      ++$rank;
    }

    # The run may retrieve a short list, but the ideal gain is up tp the
    # number of nonzero gains available in the topic
    while($rank < $num_rel
        and $ie_qrel{$topic}{$rank} != 0){
      my $ie = $ie_qrel{$topic}{$rank};
      $idcg += ($ie / (log($rank + 1)/log(2)));
      ++$rank;
    }

    if(0 != $idcg){
      $ndcg = $dcg / $idcg;
    }

    $cum{$topic}{nDCG} = $ndcg;
    $cum{nDCG} += $ndcg;
  }
}

sub means() {
  if($verbose){
    for my $topic(sort {$a cmp $b} keys %qrel){
      if(!defined $run{$topic}){
        my $p = 0;
        my $r = 0;
        my $f1 = 0;
        my $map = 0;
        my $ndcg = 0;
        printf "%s\tPrec\t%6.3f\n", $topic, $p;
        printf "%s\tRecall\t%6.3f\n", $topic, $r;
        printf "%s\tF1\t%6.3f\n", $topic, $f1;
        printf "%s\tMAP\t%6.3f\n", $topic, $map;
        printf "%s\tnDCG\t%6.3f\n", $topic, $ndcg;
        next;
      }
      my $p = $cum{$topic}{precision};
      my $r = $cum{$topic}{recall};
      my $f1 = $cum{$topic}{F1};
      my $map = $cum{$topic}{MAP};
      my $ndcg = $cum{$topic}{nDCG};
      printf "%s\tPrec\t%6.3f\n", $topic, $p;
      printf "%s\tRecall\t%6.3f\n", $topic, $r;
      printf "%s\tF1\t%6.3f\n", $topic, $f1;
      printf "%s\tMAP\t%6.3f\n", $topic, $map;
      printf "%s\tnDCG\t%6.3f\n", $topic, $ndcg;
    }
  }

  my $num_q = scalar keys %qrel;
  print "Topic Number: $num_q\n";
  printf "all\tPrec\t%6.3f\n", $cum{P}/$num_q;
  printf "all\tRecall\t%6.3f\n", $cum{R}/$num_q;
  printf "all\tF1\t%6.3f\n", $cum{F1}/$num_q;
  printf "all\tMAP\t%6.3f\n", $cum{MAP}/$num_q;
  printf "all\tnDCG\t%6.3f\n", $cum{nDCG}/$num_q;

  my $ave_prec = $cum{P} / $num_q;
  my $ave_recall = $cum{R} / $num_q;
  my $macro_f1 = 0.0;
  if($ave_prec + $ave_recall > 0){
    $macro_f1 = 2 * $ave_prec * $ave_recall / ($ave_prec + $ave_recall);
  }
  printf "all\tM-F1\t%6.3f\n", $macro_f1;

  printf "all\tM-Recall\t%6.3f\t%d\n", $total_rel_ret / $total_rel, $total_rel;
}

