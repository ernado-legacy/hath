package hath // import "cydev.ru/hath"

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestParseArgs(t *testing.T) {
	Convey("Arguments parsing", t, func() {
		input := "arg1=val1;arg2=val2;arg3=val3;arg4=4;arg5=1035567;"
		args := ParseArgs(input)
		So(args["arg1"], ShouldEqual, "val1")
		So(args["arg2"], ShouldEqual, "val2")
		So(args["arg3"], ShouldEqual, "val3")
		So(args["arg4"], ShouldEqual, "4")
		So(args["arg5"], ShouldEqual, "1035567")
		Convey("Integer", func() {
			So(args.GetInt("arg4"), ShouldEqual, 4)
		})
		Convey("String", func() {
			So(args.Get("arg1"), ShouldEqual, "val1")
		})
		Convey("Int64", func() {
			So(args.GetInt64("arg5"), ShouldEqual, 1035567)
		})
		Convey("Without terminating delimiter", func() {
			input := "arg1=val1;arg2=val2;arg3=val3;arg4=4"
			args := ParseArgs(input)
			So(args["arg1"], ShouldEqual, "val1")
			So(args["arg2"], ShouldEqual, "val2")
			So(args["arg3"], ShouldEqual, "val3")
			So(args["arg4"], ShouldEqual, "4")
		})
	})
}
