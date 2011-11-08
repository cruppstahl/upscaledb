
Summary: hamsterdb Embedded Storage
Name: hamsterdb
Version: 1.1.15
Release: 1%{?dist}
Source0: http://hamsterdb.com/public/dl/%{name}-%{version}.tar.gz
URL: http://hamsterdb.com/
License: GPL3
Group: System Environment/Libraries
BuildRequires: protobuf-devel, libtool, curl-devel
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
hamsterdb Embedded Storage is a lightweight embedded "NoSQL" 
key-value store. It is in development for more than six years 
and concentrates on ease of use, high performance, stability 
and scalability.

%package devel
Summary: C development files for the hamsterdb library
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}

%description devel
hamsterdb Embedded Storage is a lightweight embedded "NoSQL" 
key-value store. It is in development for more than five years 
and concentrates on ease of use, high performance, stability 
and scalability.

%package static
Summary: hamsterdb static libraries
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}
Requires: %{name}-devel = %{version}-%{release}

%description static
hamsterdb Embedded Storage is a lightweight embedded "NoSQL" 
key-value store. It is in development for more than five years 
and concentrates on ease of use, high performance, stability 
and scalability.


%prep
%setup -q 


%build
#autoconf
export CFLAGS="-ggdb2 -O2"
CFLAGS="$RPM_OPT_FLAGS -fno-strict-aliasing"; export CFLAGS
%configure 
make %{?_smp_mflags}


%install
rm -rf %{buildroot}
make DESTDIR=%{buildroot} prefix=%{_prefix} libdir=%{_libdir}  install
rm -fv ${RPM_BUILD_ROOT}%{_libdir}/*.la


%clean
rm -rf %{buildroot}

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%post -p /sbin/ldconfig devel

%postun -p /sbin/ldconfig devel

%post -p /sbin/ldconfig static

%postun -p /sbin/ldconfig static


%files
%defattr(-,root,root,-)
%doc AUTHORS COPYING.GPL3 COPYING.GPL2 README
%{_bindir}/*
%{_libdir}/*.so.*

%files devel
%defattr(-,root,root,-)
%doc	documentation/*
%{_libdir}/*.so
%{_includedir}/ham

%files static
%defattr(-,root,root,-)
%{_libdir}/libhamsterdb.a
%{_libdir}/libhamserver.a




%changelog
* Sat Jan 29 2011 hamster@gunkel.ca - 1.1.8-1
- initial version of spec file
- rest of changelog goes here

