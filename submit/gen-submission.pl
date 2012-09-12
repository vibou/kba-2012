#!/usr/bin/env perl

#
# Generate the final submit run
#
# Version: 1.1.
#
# Usage: gen-submission.pl [-v] <runfile> <savefile>\n";
#        -v: verbose mode (default: no)
#        <runfile> may be gzipped.
#

use strict;
use Getopt::Long;

my $usage = "gen-submission.pl [-v] <runfile> <savefile>\n";
my $verbose = 0;
GetOptions('verbose!' => \$verbose,
    ) or die $usage;

# allow qrels and runfiles to be compressed with gzip
@ARGV = map { /.gz$/ ? "gzip -dc $_ |" : $_ } @ARGV;

my $run_file = shift or die $usage;
my $save_file = shift or die $usage;

my %run;

my $team_id = "UDel";
my $system_id = "exact-match";
#my $system_id = "wiki-match";

main();

sub main(){
  load_run();
  save_run();
}

# load the data of runfile
sub load_run(){
  open RUN, $run_file or die "Can't open `$run_file': $!\n";
  print "Loading $run_file\n";
  while (<RUN>) {
    chomp;
    next if /^$/;

    my ($query, $did, $score) = split;
    $run{$query}{$did} = $score;
  }
  close RUN;
}

sub save_run(){
  open SAVE, ">" . $save_file or die "Can't open `$save_file': $!\n";
  print "Saving $save_file\n";
  
  for my $query(sort {$a cmp $b} keys %run){
    for my $did(sort {$a cmp $b} keys %{$run{$query}}){
      my $score = $run{$query}{$did};
      print SAVE "$team_id $system_id $did $query $score\n";
    }
  }

  close SAVE;
}

