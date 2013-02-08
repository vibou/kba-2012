#!/usr/bin/env perl

#
# Use the judgment to search for the optimal filtering threshold on the traing
# data and apply to the testing data
#
# Version: 1.1.
#
# Usage: opt-thred-filter.pl [-v] <qrels> <train> <test> <run>\n";
#        -v: verbose mode (default: no)
#        <qrels>, <train>, <test>, <run> may be gzipped.
#

use strict;
use POSIX;
use Getopt::Long;

my $usage = "opt-thred-filter.pl [-v] <qrels> <train> <test> <run>\n";
my $verbose = 0;
GetOptions('verbose!' => \$verbose,
    ) or die $usage;

# allow qrels and runfiles to be compressed with gzip
@ARGV = map { /.gz$/ ? "gzip -dc $_ |" : $_ } @ARGV;

my $qrels_file = shift or die $usage;
my $train_file = shift or die $usage;
my $test_file = shift or die $usage;
my $run_file = shift or die $usage;

my %qrel;
my %train_run;
my %test_run;
my %train_filter_run;
my %test_filter_run;
my %opt_thred;
my %eval;

my $INC_STEP = 1.0;

main();

sub main(){
  load_qrels();
  load_train();
  load_test();
  search_opt_thred();
  apply_opt_filter();
}

# load qrel data
sub load_qrels(){
  open QRELS, $qrels_file or die "Can't open `$qrels_file': $!\n";
  print "Loading $qrels_file\n";

  while (<QRELS>) {
    chomp;
    next if /^$/;

    my (undef, undef, $did, $query, $score, $rel, $const) = split;
    my ($epoch, undef) = split /-/, $did;
    # this is the epoch time for the last second of 2011, i.e. Dec 31 2011
    # 23:59:59 GMT+0000
    # we only load the judgment for training data
    next if $epoch > 1325375999;
    # alternatively, we would load the judgment for testing data
    #next if $epoch <= 1325375999;

    if($score > 0 and $rel > 0){
      $qrel{$query}{$did} = $rel;
    }
  }
  close QRELS;
}

# load the data of training data
sub load_train(){
  open TRAIN, $train_file or die "Can't open `$train_file': $!\n";
  print "Loading $train_file\n";

  while (<TRAIN>) {
    chomp;
    next if /^$/;
    
    my ($lead, $query, $did, $score) = split / - /;
    $train_run{$query}{$did} = $score;
  }
  close TRAIN;
}

# load the data of testing data
sub load_test(){
  open TEST, $test_file or die "Can't open `$test_file': $!\n";
  print "Loading $test_file\n";

  while (<TEST>) {
    chomp;
    next if /^$/;
    
    my ($lead, $query, $did, $score) = split / - /;
    $test_run{$query}{$did} = $score;
  }
  close TEST;
}

sub search_opt_thred(){
  my $max_score = 0.0;
  my $min_score = 0.0;
  # first, get the minimum and maximum of document score
  for my $query(keys %train_run){
    for my $did(keys %{$train_run{$query}}){
      my $score = $train_run{$query}{$did};
      if($score > $max_score){
        $max_score = $score;
        next;
      }
      if($score < $min_score){
        $min_score = $score;
      }
    }
  }

  my $min_thred = ceil($min_score);
  my $max_thred = floor($max_score);
  if($max_thred > 150){
    $max_thred = 150;
  }

  printf "MIN_THRED: %d\n", $min_thred;
  printf "MAX_THRED: %d\n", $max_thred;

  for my $query(keys %train_run){
    $opt_thred{$query}{F1} = 0;
    $opt_thred{$query}{THRED} = $min_thred;
  }

  # then, try different threshold varying from the minimum to the maixmum
  for(my $thred = $min_thred; $thred <= $max_thred; $thred += $INC_STEP){
    printf "CUR_THRED: %d\n", $thred;

    # clear the previous filtered training results
    for(keys %train_filter_run){
      delete $train_filter_run{$_};
    }

    # filter the results using the current threshold
    for my $query(keys %train_run){
      for my $did(keys %{$train_run{$query}}){
        my $score = $train_run{$query}{$did};
        if($score >= $thred){
          $train_filter_run{$query}{$did} = $score;
        }
      }
    }

    # apply the evaluation
    pr_eval();

    # check whether the current thred can lead to better performance
    for my $query(keys %eval){
      my $cur_f1 = $eval{$query}{F1};
      my $opt_f1 = $opt_thred{$query}{F1};
      if($cur_f1 > $opt_f1){
        $opt_thred{$query}{F1} = $cur_f1;
        $opt_thred{$query}{THRED} = $thred;
      }
    }
  }
  
  for my $query(keys %test_run){
    my $thred = $opt_thred{$query}{THRED};
    for my $did(keys %{$test_run{$query}}){
      my $score = $test_run{$query}{$did};
      if($score >= $thred){
        $test_filter_run{$query}{$did} = $score;
      }
    }
  }
}

sub apply_opt_filter(){
  open FILE, ">" . $run_file or die "Can not open `$run_file': $!\n";
  print "Saving $run_file\n";

  for my $query(sort {$a cmp $b} keys %test_filter_run){
    for my $did(keys %{$test_filter_run{$query}}){
      my $score = $test_filter_run{$query}{$did};
      print FILE "$query $did $score\n";
    }
  }

  close FILE;
}

# evaluate by precision, recall and F1
sub pr_eval(){
  for my $query(sort {$a cmp $b} keys %qrel){
    my $num_ret = scalar keys %{$train_filter_run{$query}};
    my $num_rel = scalar keys %{$qrel{$query}};

    if(0 == $num_ret){
      $num_ret = 1;
    }
    if(0 == $num_rel){
      $num_rel = 1;
    }

    my $rel_ret = 0;
    for my $did(keys %{$train_filter_run{$query}}){
      next if !defined $qrel{$query}{$did};
      ++$rel_ret;
    }

    my $precision = $rel_ret / $num_ret;
    my $recall = $rel_ret / $num_rel;
    my $f1 = 0;
    if(0 < $precision + $recall){
      $f1 = 2 * $precision * $recall / ($precision + $recall);
    }
    $eval{$query}{precision} = $precision;
    $eval{$query}{recall} = $recall;
    $eval{$query}{F1} = $f1;
  }
}

