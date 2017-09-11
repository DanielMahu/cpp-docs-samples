#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "data.h"

using google::bigtable::v2::Bigtable;
using google::bigtable::v2::ReadRowsResponse;
using testing::_;
using testing::Invoke;
using testing::ReturnPointee;

// TODO(dmahu): this is copied from "grpc++/test/mock_stream.h"
template <class R>
class MockClientReader : public grpc::ClientReaderInterface<R> {
 public:
  MockClientReader() = default;

  /// ClientStreamingInterface
  MOCK_METHOD0_T(Finish, grpc::Status());

  /// ReaderInterface
  MOCK_METHOD1_T(NextMessageSize, bool(uint32_t*));
  MOCK_METHOD1_T(Read, bool(R*));

  /// ClientReaderInterface
  MOCK_METHOD0_T(WaitForInitialMetadata, void());
};


class TableForTesting : public bigtable::Table {
 public:
  TableForTesting()
      : bigtable::Table(nullptr, "") { }

  // Acts as if the responses in the fixture were received over the
  // wire, and fills rows with the callback arguments. If stop_after
  // is non-zero, returns false from the callback after stop_after
  // invocations.
  grpc::Status ReadRowsFromFixture(const std::vector<ReadRowsResponse>& fixture,
                                   std::vector<bigtable::RowPart>* rows,
                                   int stop_after) {
    MockClientReader<ReadRowsResponse> stream;
    grpc::Status stream_status;

    int read_count = 0;
    auto stream_read = [&stream, &read_count, &fixture]
                       (ReadRowsResponse* response) {
      if (read_count < fixture.size()) {
        *response = fixture[read_count];
      }
      return read_count++ < fixture.size();
    };

    EXPECT_CALL(stream, Finish())
        .WillOnce(ReturnPointee(&stream_status));
    EXPECT_CALL(stream, Read(_))
        .Times(fixture.size() + 1)
        .WillRepeatedly(Invoke(stream_read));

    auto process_row = [&rows, stop_after](const bigtable::RowPart &r) {
      rows->push_back(r);
      return rows->size() != stop_after;
    };
    auto request_cancel = [&stream_status]() {
      // after a cancel is requested, typically the stream returns this error:
      stream_status = grpc::Status(grpc::CANCELLED, "");
    };

    return ReadRowsFromStream(&stream, process_row, request_cancel);
  }
};


TEST(DataTest, ReadRowsEmptyReply) {
  TableForTesting table;
  std::vector<ReadRowsResponse> fixture;
  std::vector<bigtable::RowPart> rows;

  grpc::Status s = table.ReadRowsFromFixture(fixture, &rows, 0);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(rows.size(), 0);
}

TEST(DataTest, ReadRowsSingleRowSingleChunk) {
  TableForTesting table;
  std::vector<ReadRowsResponse> fixture;
  std::vector<bigtable::RowPart> rows;

  ReadRowsResponse rrr;
  auto chunk = rrr.add_chunks();
  chunk->set_row_key("r1");
  chunk->mutable_family_name()->set_value("fam");
  chunk->mutable_qualifier()->set_value("qual");
  chunk->set_timestamp_micros(42000);
  chunk->set_commit_row(true);

  fixture.push_back(rrr);

  grpc::Status s = table.ReadRowsFromFixture(fixture, &rows, 0);

  EXPECT_TRUE(s.ok());
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0].row(), "r1");
}
