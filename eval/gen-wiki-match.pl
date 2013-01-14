#!/usr/bin/env perl

#
# Generrate the final results of wiki-match
#
# Version: 1.1.
#
# Usage: gen-wiki-match.pl [-v] <qrels> <runfile> <save>\n";
#        -v: verbose mode (default: no)
#        <qrels> and <runfile> may be gzipped.
#

use strict;
use Getopt::Long;

my $usage = "gen-wiki-match.pl [-v] <qrels> <runfile> <save>\n";
my $verbose = 0;
GetOptions('verbose!' => \$verbose,
    ) or die $usage;

# allow qrels and runfiles to be compressed with gzip
@ARGV = map { /.gz$/ ? "gzip -dc $_ |" : $_ } @ARGV;

my $qrels_file = shift or die $usage;
my $run_file = shift or die $usage;
my $save_file = shift or die $usage;

my %qrel;
my %run;
my %rel_run;

# the threshold of score for the relevant documents
#my $REL_THRED = 101;
my $REL_THRED = 99;

main();

sub main(){
  load_qrels();
  load_run();
  gen_result();
  save_results();
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

    #s/\-(\d+) $/\- $1/g;
    my ($lead, $query, $did, $score) = split / - /;
    $run{$query}{$did} = $score;
  }
  close RUN;
}

# generate results
sub gen_result(){
  my $sum_all = 0;
  my $num_all = 0;
  for my $query(sort {$a cmp $b} keys %qrel){
    my $sum_score = 0;
    my $num_score = 0;
    for my $did(keys %{$run{$query}}){
      my $score = $run{$query}{$did};
      if($score > $REL_THRED){
        $rel_run{$query}{$did} = $score;
      }
      if($score >= 100){
        $score -= 100;
      }
      $sum_score += $score;
      ++$num_score;
    }
    #my $avg_score = $sum_score / $num_score;
    #printf "%32s\t%.3f\n", $query, $avg_score;
    #$sum_all += $avg_score;
    ++$num_all;
  }
  #my $query = "all";
  #my $avg_score = $sum_all / $num_all;
  #printf "%32s\t%.3f\n", $query, $avg_score;
}

#save the results
sub save_results(){
  open FILE, ">" . $save_file or die "Can not open `$save_file': $!\n";
  print "Saving $save_file\n";

  for my $query(sort {$a cmp $b} keys %rel_run){
    for my $did(keys %{$rel_run{$query}}){
      my $score = $rel_run{$query}{$did};
      print FILE "$query $did $score\n";
    }
  }

  close FILE;
}

