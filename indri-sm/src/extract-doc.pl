#!/usr/bin/env perl

#
# extract the retrieved documents in the corpus
#

use strict;
use Getopt::Long;
use lib '/usr/local/perl/lib/site_perl/5.8.8/';
#use DateTime;

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
   
    # Thanks to http://search.cpan.org/~drolsky/DateTime-0.78/lib/DateTime.pm
    #my $dt = DateTime->from_epoch( epoch => $epoch );
    #my $corpus_file = sprintf("%4d-%2d-%2d-%2d", $dt->year, $dt->month,
    #$dt->day, $dt->hour);
    
    # Thanks to http://www.epochconverter.com/programming/functions-perl.php
    # since our timezone is GMT-4, we need to calibrate it to GMT time
    $epoch = $epoch + 14400;
    my($sec, $min, $hour, $day, $month, $year) = (localtime($epoch))[0,1,2,3,4,5];
    $year = $year + 1900;
    $month = $month + 1;
    my $corpus_file = sprintf("%04d-%02d-%02d-%02d", $year, $month, $day, $hour);

    $ret_list{$qid}{$rank} = $did;
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
    my $raw_file = "$corpus_dir/$file";
    #print "Parsing $raw_file\n";
    parse_raw_file($file, $raw_file);
    #last;
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
  my $has_found = 0;

  while(<RAW>){
    chomp;
    next if /^$/;
    next if /<TEXT>/;
    next if /<\/TEXT>/;

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
          $has_found = 1;
        }

        # reset the intermediate variables
        $doc = "";

        next;
      }else{
        $doc = $doc . $_ ."\n";
      }
    }
  }

  unless(1 == $has_found){
    print "No document found in $raw_file\n";
  }
  close RAW;
}

sub save_docs(){
  open CORPUS, ">" . $save_file 
    or die "Can't open `$save_file': $!\n";
  print "Saving $save_file\n";

  for my $qid(sort {$a<=>$b} keys %ret_list){
    for my $rank(sort {$a<=>$b} keys %{$ret_list{$qid}}){
      my $did = $ret_list{$qid}{$rank};
      my $doc = $doc_list{$did};

      next if !defined $doc;
      print CORPUS "<DOC>\n";
      print CORPUS "<DOCNO> $did <\/DOCNO>\n";
      print CORPUS "<QUERY> $qid-$rank <\/QUERY>\n";
      print CORPUS "<TEXT>\n";
      print CORPUS "\n$doc\n\n";
      print CORPUS "<\/TEXT>\n";
      print CORPUS "<\/DOC>\n";
    }
  }

  close CORPUS;
}


