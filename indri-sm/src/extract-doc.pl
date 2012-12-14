#!/usr/bin/env perl

#
# extract the retrieved documents in the corpus
#

use strict;
use Getopt::Long;
use DateTime;

my $script_name = "extract-doc.pl <ret-list> <save>";
my $usage = "$script_name [-v]\n";

my $verbose = 0;
GetOptions('verbose!' => \$verbose,
  ) or die $usage;

my %ret_list;
my %rev_ret_list;
my %doc_list;

my $corpus_dir = "corpus/";

my $ret_list_file = shift or die $usage;
my $save_file = shift or die $usage;

main();

sub main(){
  load_ret_list();
  parse_corpus();
  save_docs();
}

sub load_ret_list(){
  open RET, $ret_list_file or die "Can't open `$ret_list_file': $!\n";
  print "Loading $ret_list_file\n";

  while(<RET>){
    chomp;
    next if /^$/;

    my ($qid, undef, $did, $rank, $score, undef) = split;
    my ($epoch, $md5) = split /-/, $did;
    my $dt = DateTime->from_epoch( epoch => $epoch );
    my $corpus_file = sprintf("%4d-%2d-%2d-%2d", $dt->year, $dt->month,
      $dt->day, $dt->hour);

    $ret_list{$qid}{$did} = 1;
    $rev_ret_list{$corpus_file}{$did}{$qid} = 1;
  }

  close MAP;
}

sub parse_corpus(){
  # open the root directory
  opendir(my $dh, $corpus_dir) or die "Can't opendir `$corpus_dir': $!\n";
  my @files = grep { !/^\./ && -f "$corpus_dir/$_" } readdir($dh);
  closedir $dh;
  print @files . " files in total in $corpus_dir\n";

  for my $file (sort {$a cmp $b} @files){
    unless(defined $rev_ret_list{$file}){
      print "Skipping $file\n";
      next;
    }
    my $raw_file = "$dir/$file";
    parse_raw_file($file, $raw_file);
  }
}

# parse one raw file in TREC format
sub parse_raw_file(){
  my $usage = "parse_raw_file(\$file, \$raw_file)\n";
  my $file = shift or die $usage;
  my $raw_file = shift or die $usage;

  open RAW, $raw_file or die "Can not open `$raw_file': $!\n";
  print "Loading $raw_file\n";

  my %docs;
  my $doc = "";
  my $did;
  my $is_in_doc = 0;

  while(<RAW>){
    chomp;
    next if /^$/;

    if($_ =~ m/<DOCNO>(.*)<\/DOCNO>/){
      $did = $1;
      $did =~ s/^\s+//;
      $did =~ s/\s+$//;
      next;
    }

    if($_ =~ m/<DOC>/){
      $is_in_doc = 1;
      next;
    }

    if(1 == $is_in_doc){
      if($_ =~ m/<\/DOC>/){
        $is_in_doc = 0;

        # check whether any entity is mapped to the document
        if(defined $rev_ret_list{$file}{$did}){
          $doc_list{$did} = $doc;
        }

        # reset the intermediate variables
        $doc = "";

        next;
      }else{
        $doc = $doc . $_ ."\n";
      }
    }
  }

  close RAW;
}


