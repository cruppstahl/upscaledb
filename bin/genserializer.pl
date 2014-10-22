#!/usr/bin/perl 

use strict;
use Switch;
use Data::Dumper;

my %options = (
        prefix => '',
   );

sub print_prologue
{
  while (<>) {
    return if (/^PROLOGUE_END/);
    print;
  }
}

sub print_epilogue
{
  while (<>) {
    return if (/^EPILOGUE_END/);
    print;
  }
}

sub print_stdlib
{
  my $p = $options{'prefix'};
  print "template<typename Ex, typename In>\n";
  print "struct $p" . "_Base {\n";
  print "  Ex value;\n\n";
  print "  $p" . "_Base() {\n";
  print "    clear();\n";
  print "  }\n\n";
  print "  $p" . "_Base(const Ex &t)\n";
  print "    : value((In)t) {\n";
  print "  }\n\n";
  print "  operator Ex() {\n";
  print "    return (value);\n";
  print "  }\n\n";
  print "  void clear() {\n";
  print "    value = (Ex)0;\n";
  print "  }\n\n";
  print "  size_t get_size() const {\n";
  print "    return (sizeof(In));\n";
  print "  }\n\n";
  print "  void serialize(unsigned char **pptr, int *psize) const {\n";
  print "    *(In *)*pptr = (In)value;\n";
  print "    *pptr += sizeof(In);\n";
  print "    *psize -= sizeof(In);\n";
  print "    assert(*psize >= 0);\n";
  print "  }\n\n";
  print "  void deserialize(unsigned char **pptr, int *psize) {\n";
  print "    value = (Ex) *(In *)*pptr;\n";
  print "    *pptr += sizeof(In);\n";
  print "    *psize -= sizeof(In);\n";
  print "    assert(*psize >= 0);\n";
  print "  }\n";
  print "};\n\n";
  print "struct $p" . "Bytes {\n";
  print "  uint8_t *value;\n";
  print "  uint32_t size;\n\n";
  print "  $p" . "Bytes() {\n";
  print "    clear();\n";
  print "  }\n\n";
  print "  size_t align(size_t s) const {\n";
  print "    if (s % 4) return (s + 4 - (s % 4));\n";
  print "    return (s);\n";
  print "  }\n\n";
  print "  void clear() {\n";
  print "    value = 0; size = 0;\n";
  print "  }\n\n";
  print "  size_t get_size() const {\n";
  print "    return (sizeof(uint32_t) + align(size)); // align to 32bits\n";
  print "  }\n\n";
  print "  void serialize(unsigned char **pptr, int *psize) const {\n";
  print "    *(uint32_t *)*pptr = size;\n";
  print "    *pptr += sizeof(uint32_t);\n";
  print "    *psize -= sizeof(uint32_t);\n";
  print "    if (size) {\n";
  print "      memcpy(*pptr, value, size);\n";
  print "      *pptr += align(size); // align to 32bits\n";
  print "      *psize -= align(size);\n";
  print "      assert(*psize >= 0);\n";
  print "    }\n";
  print "  }\n\n";
  print "  void deserialize(unsigned char **pptr, int *psize) {\n";
  print "    size = *(uint32_t *)*pptr;\n";
  print "    *pptr += sizeof(uint32_t);\n";
  print "    *psize -= sizeof(uint32_t);\n";
  print "    if (size) {\n";
  print "      value = *pptr;\n";
  print "      *pptr += align(size); // align to 32bits\n";
  print "      *psize -= align(size);\n";
  print "      assert(*psize >= 0);\n";
  print "    }\n";
  print "    else\n";
  print "      value = 0;\n";
  print "  }\n";
  print "};\n\n";
  print "typedef $p" . "_Base<bool, uint32_t> $p" . "Bool;\n";
  print "typedef $p" . "_Base<uint8_t, uint32_t> $p" . "Uint8;\n";
  print "typedef $p" . "_Base<uint16_t, uint32_t> $p" . "Uint16;\n";
  print "typedef $p" . "_Base<uint32_t, uint32_t> $p" . "Uint32;\n";
  print "typedef $p" . "_Base<int8_t, int32_t> $p" . "Sint8;\n";
  print "typedef $p" . "_Base<int16_t, int32_t> $p" . "Sint16;\n";
  print "typedef $p" . "_Base<int32_t, int32_t> $p" . "Sint32;\n";
  print "typedef $p" . "_Base<uint64_t, uint64_t> $p" . "Uint64;\n";
  print "typedef $p" . "_Base<int64_t, int64_t> $p" . "Sint64;\n";
  print "\n\n";
}

sub read_custom_implementation
{
  my $custom = '';
  while (<>) {
    return $custom if (/CUSTOM_IMPLEMENTATION_END/);
    $custom .= $_;
  }
}

sub set_option
{
  my $name = shift;
  my $value = shift;
  $options{$name} = $value;
}

sub convert_type
{
  my $t = shift;
  my $prefix = $options{'prefix'};

  switch ($t) {
    case 'bool'   { return $prefix . 'Bool'; }
    case 'uint8'  { return $prefix . 'Uint8'; }
    case 'sint8'  { return $prefix . 'Sint8'; }
    case 'uint16' { return $prefix . 'Uint16'; }
    case 'sint16' { return $prefix . 'Sint16'; }
    case 'uint32' { return $prefix . 'Uint32'; }
    case 'sint32' { return $prefix . 'Sint32'; }
    case 'uint64' { return $prefix . 'Uint64'; }
    case 'sint64' { return $prefix . 'Sint64'; }
    case 'string' { return $prefix . 'String'; }
    case 'bytes'  { return $prefix . 'Bytes'; }
    else          { return $prefix . $t; }
  }
}

sub print_message
{
  my $name = shift;
  my $custom;
  my @fields;
  while (<>) {
    chomp;
    if (/optional +(\w+) +(\w+?);/) {
      my %h = ('type' => convert_type($1), 'name' => $2, 'optional' => 1);
      push(@fields, \%h);
      next;
    }
    if (/(\w+) +(\w+?);/) {
      my %h = ('type' => convert_type($1), 'name' => $2, 'optional' => 0);
      push(@fields, \%h);
      next;
    }
    if (/CUSTOM_IMPLEMENTATION_BEGIN/) {
      $custom = read_custom_implementation();
      next;
    }
    if (/^MESSAGE_END/) {
      print_message_code($name, \@fields, $custom);
      return;
    }
  }
}

sub print_message_code
{
  my $name = shift;
  my $prefix = $options{'prefix'};
  my $fields = shift;
  my $custom = shift;
  print "struct $prefix$name {\n";

  # fields
  foreach (@$fields) {
    if ($$_{'optional'}) {
      print "  $prefix" . 'Bool has_' . $$_{'name'} . ";\n";
    }
    print '  ' . $$_{'type'} . ' ' . $$_{'name'} . ";\n";
  }
  print "\n";

  # constructor
  print "  $prefix$name() {\n";
  print "    clear();\n";
  print "  }\n\n";

  # custom implementation?
  if ($custom) {
    print $custom;
    print "};\n\n";
    return;
  }

  # get_size
  print "  size_t get_size() const {\n";
  print "    return (\n";
  foreach (@$fields) {
    my $name = $$_{'name'};
    if ($$_{'optional'}) {
      print "          has_$name.get_size() + \n";
      print "          (has_$name.value ? $name.get_size() : 0) + \n";
    }
    else {
      print "          $name.get_size() + \n";
    }
  }
  print "          0);\n";
  print "  }\n\n";

  # clear
  print "  void clear() {\n";
  foreach (@$fields) {
    my $name = $$_{'name'};
    if ($$_{'optional'}) {
      print "    has_$name = false;\n";
    }
    print "    $name.clear();\n";
  }
  print "  }\n\n";

  # serialize
  print "  void serialize(unsigned char **pptr, int *psize) const {\n";
  foreach (@$fields) {
    my $name = $$_{'name'};
    if ($$_{'optional'}) {
      print "    has_$name.serialize(pptr, psize);\n";
      print "    if (has_$name.value) $name.serialize(pptr, psize);\n";
    }
    else {
      print "    $name.serialize(pptr, psize);\n";
    }
  }
  print "  }\n\n";

  # deserialize
  print "  void deserialize(unsigned char **pptr, int *psize) {\n";
  foreach (@$fields) {
    my $name = $$_{'name'};
    if ($$_{'optional'}) {
      print "    has_$name.deserialize(pptr, psize);\n";
      print "    if (has_$name.value) $name.deserialize(pptr, psize);\n";
    }
    else {
      print "    $name.deserialize(pptr, psize);\n";
    }
  }
  print "  }\n";
  print "};\n\n";
}

while (<>) {
  chomp;
  if (/^PROLOGUE_BEGIN/) {
    print_prologue();
    print_stdlib();
    next;
  }
  if (/^EPILOGUE_BEGIN/) {
    print_epilogue();
    next;
  }
  if (/^SET_OPTION\((\w+), (.*?)\)/) {
    set_option($1, $2);
    next;
  }
  if (/^MESSAGE_BEGIN\((.*?)\)/) {
    print_message($1);
    next;
  }
}
