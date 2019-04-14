package topk

import (
	"testing"
)

func Test_FileBucket(t *testing.T) {
	path := "./test_result/test"
	fb, err := InitFileBucket(path, 1024)
	if err != nil {
		t.Errorf("InitFileBucket Failed. %v", err)
	}
	defer fb.Close()

	lines := [...]string{
		"http://farm1.static.flickr.com/119/288329997_19ebf1d7b3_o.jpg",
		"http://farm1.static.flickr.com/35/99344798_f2ad604eda_o.jpg",
		"http://2.bp.blogspot.com/_DxSfGxvclek/SH-K403dDuI/AAAAAAAAAC8/6TGJ67y8kZk/s320/Aaron%2BEckhart.jpg",
		"http://2.bp.blogspot.com/_biK-MLwOHEc/RtXJ3nA4y5I/AAAAAAAAALM/fMGxP5sRK10/s320/aaroneckhart.jpg",
		"http://3.bp.blogspot.com/_bto58WjLomw/Rk1yrB-rEXI/AAAAAAAABGo/dy33tJJsjSE/s400/aaron_eckhart3.jpg",
		"http://3.bp.blogspot.com/_39-VfFoO9u0/SOTdltmCwrI/AAAAAAAAAvs/X2srWzTFzj0/s200/aaron-eckhart.jpg",
		"http://4.bp.blogspot.com/_yioIuRi4L1s/SI5fCNMfFoI/AAAAAAAACgc/pnT74rs6yMI/s400/aaron%2Beckhart.jpg",
		"http://4.bp.blogspot.com/_XsPzMgto29Q/SKLcwwSEQaI/AAAAAAAAAOQ/qVhy3sfEWJo/s320/Aaron_Eckhart.jpg",
		"http://4.bp.blogspot.com/_BfvQLEzMsbA/RrCJIRrMd1I/AAAAAAAAABg/oEhav2fakdM/s200/ibelieveinharvydent.jpg",
		"http://4.bp.blogspot.com/_FGrxKMwdUu8/SH-8zcsJ0-I/AAAAAAAADJ4/Q29KN7ZSrgs/s320/Meet%2BBill.jpg",
	}
	for _, l := range lines {
		if err = fb.Write(l + "\n"); err != nil {
			t.Errorf("Write failed.%v", err)
		}
	}
	if err = fb.Flush(); err != nil {
		t.Fatalf("Flush failed. %v", err)
	}

	tuples, _, err := fb.Statistic()
	if err != nil {
		t.Errorf("Statistic failed. %v", err)
	}

	if len(tuples) != 10 {
		t.Fatalf("DataSet not right. %d, %v", len(tuples), tuples)
	}
}
