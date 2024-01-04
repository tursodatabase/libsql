require('vstudio')
require('gmake2')

premake.api.register {
  name = "wxUseProps",
  scope = "project",
  kind = "boolean",
  default = false
}

local function wxPropertySheets(prj)
--  if premake.wxProject ~= nil and premake.wxProject then 
  if prj.wxUseProps then 
    premake.push('<ImportGroup Label="PropertySheets">')
    if premake.wxSetupProps ~= nil and premake.wxSetupProps ~= '' then
      premake.w('<Import Project="' .. premake.wxSetupProps .. '" />')
    else
      premake.w('<Import Project="wx_setup.props" />')
    end
    premake.w('<Import Project="wx_local.props" Condition="Exists(\'wx_local.props\')" />')
    premake.pop('</ImportGroup>')
  end
end

premake.override(premake.vstudio.vc2010.elements, "project", function(base, prj)
	local calls = base(prj)
	table.insertafter(calls, premake.vstudio.vc2010.importExtensionSettings, wxPropertySheets)
	return calls
end)

premake.override(premake.modules.gmake2, "target", function(base, cfg, toolset)
  local targetpath = string.gsub(premake.project.getrelative(cfg.project, cfg.buildtarget.directory), ' ', '_')
  premake.outln('TARGETDIR = ' .. targetpath)
  premake.outln('TARGET = $(TARGETDIR)/' .. cfg.buildtarget.name)
end)
  
premake.override(premake.modules.gmake2, "objdir", function(base, cfg, toolset)
  local objpath = string.gsub(premake.project.getrelative(cfg.project, cfg.objdir), ' ', '_')
  premake.outln('OBJDIR = ' .. objpath)
end)

-- Determine version of Visual Studio action
vc_version = "";
if _ACTION == "vs2003" then
  vc_version = 7
elseif _ACTION == "vs2005" then
  vc_version = 8
elseif _ACTION == "vs2008" then
  vc_version = 9
elseif _ACTION == "vs2010" then
  vc_version = 10
elseif _ACTION == "vs2012" then
  vc_version = 11
elseif _ACTION == "vs2013" then
  vc_version = 12
elseif _ACTION == "vs2015" then
  vc_version = 14
elseif _ACTION == "vs2017" then
  vc_version = 15
elseif _ACTION == "vs2019" then
  vc_version = 16
elseif _ACTION == "vs2022" then
  vc_version = 17
end

is_msvc = false
msvc_useProps = false
if ( vc_version ~= "" ) then
  is_msvc = true
  msvc_useProps = vc_version >= 10
  vc_with_ver = "vc"..vc_version
end

function wxWorkspaceCommon()
  configurations { "Debug", "Release" }
  platforms { "Win32", "Win64" }
  location(BUILDDIR)

  defines {
    "_WINDOWS",
    "WIN32",
    "_CRT_SECURE_NO_WARNINGS",
    "_CRT_SECURE_NO_DEPRECATE",
    "_CRT_NONSTDC_NO_WARNINGS",
    "_CRT_NONSTDC_NO_DEPRECATE"
  }

  filter { "platforms:Win32" }
    system "Windows"
    architecture "x32"

  filter { "platforms:Win64" }
    system "Windows"
    architecture "x64"
    targetsuffix "_x64"

  filter { "configurations:Debug*" }
    defines {
      "DEBUG",
      "_DEBUG"
    }
    symbols "On"

  filter { "configurations:Release*" }
    defines {
      "NDEBUG"
    }
    optimize "On"

  filter {}
end
