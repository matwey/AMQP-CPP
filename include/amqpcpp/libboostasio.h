#pragma once

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/write.hpp>


namespace AMQP {

class LibBoostAsioChannel;

template<class Stream>
class LibBoostAsioConnectionImpl:
	public std::enable_shared_from_this<LibBoostAsioConnectionImpl<Stream>>,
	private ConnectionHandler {
public:
	using executor_type = typename Stream::executor_type;
	using next_layer_type = std::decay_t<Stream>;
	using lowest_layer_type = typename next_layer_type::lowest_layer_type;

private:
	void onData(Connection* connection, const char* data, size_t size) override final {
		boost::system::error_code ec;

		/* Sync write is used here since there is no other way for back-pressure. */
		boost::asio::write(_stream, boost::asio::buffer(data, size), ec);

		if (ec) {
			connection->fail(ec.message().c_str());
		}
	}

	uint16_t onNegotiate(Connection *connection, uint16_t interval) override final {
		if (interval == 0) return 0;

		const auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::seconds(interval)) / 2;
		do_wait(timeout);

		return interval;
	}

	void onClosed(Connection *connection) override final {
		boost::system::error_code ec;

		_stream.lowest_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
		_stream.lowest_layer().close(ec);
	}

	void do_read() {
		using namespace std::placeholders;

		auto self = this->shared_from_this();

		boost::asio::async_read(
			_stream,
			_rx_streambuf,
			boost::asio::transfer_at_least(_connection.expected() - _rx_streambuf.size()),
			boost::asio::bind_executor(_strand, std::bind(&LibBoostAsioConnectionImpl::handle_read, std::move(self), _1, _2))
		);
	}

	void handle_read(const boost::system::error_code& ec, std::size_t transferred_bytes) {
		if (ec) {
			_connection.fail(ec.message().c_str());
			return;
		}

		std::size_t consumed_total = 0;

		for (const auto& buffer: _rx_streambuf.data()) {
			consumed_total += _connection.parse(reinterpret_cast<const char*>(buffer.data()), buffer.size());
		}

		_rx_streambuf.consume(consumed_total);

		do_read();
	}

	void do_wait(std::chrono::milliseconds timeout) {
		using namespace std::placeholders;

		auto self = this->shared_from_this();

		_heartbeat.expires_after(timeout);
		_heartbeat.async_wait(boost::asio::bind_executor(_strand, std::bind(&LibBoostAsioConnectionImpl::handle_wait, std::move(self), _1, timeout)));
	}

	void handle_wait(const boost::system::error_code& ec, std::chrono::milliseconds timeout) {
		if (ec) {
			_connection.fail(ec.message().c_str());
			return;
		}

		_connection.heartbeat();
		do_wait(timeout);
	}

public:
	template<class... Args>
	LibBoostAsioConnectionImpl(Stream&& stream, Args&&... args):
		_stream{std::forward<Stream>(stream)},
		_strand{_stream.get_executor()},
		_heartbeat{_stream.get_executor()},
		_connection{this, std::forward<Args>(args)...},
		_rx_streambuf{_connection.maxFrame()} {}

	void start() {
		do_read();
	}

	LibBoostAsioConnectionImpl(const LibBoostAsioConnectionImpl&) = delete;
	LibBoostAsioConnectionImpl(LibBoostAsioConnectionImpl&&) = delete;
	LibBoostAsioConnectionImpl& operator=(const LibBoostAsioConnectionImpl&) = delete;
	LibBoostAsioConnectionImpl& operator=(LibBoostAsioConnectionImpl&&) = delete;

	template<class... Args>
	static std::shared_ptr<LibBoostAsioConnectionImpl<Stream>> create(Stream&& stream, Args&&... args) {
		auto implementation = std::make_shared<LibBoostAsioConnectionImpl<Stream>>(std::forward<Stream>(stream), std::forward<Args>(args)...);

		/* Two-phase construction is required here since shared_from_this() is not available in constructor */
		implementation->start();

		return implementation;
	}

	Connection& connection() noexcept {
		return _connection;
	}

	const Connection& connection() const noexcept {
		return _connection;
	}

	const executor_type& get_executor() noexcept {
		return _stream.get_executor();
	}

	lowest_layer_type& lowest_layer() noexcept {
		return _stream.lowest_layer();
	}

	const lowest_layer_type& lowest_layer() const noexcept {
		return _stream.lowest_layer();
	}

	next_layer_type& next_layer() noexcept {
		return _stream;
	}

	const next_layer_type& next_layer() const noexcept {
		return _stream;
	}

	virtual ~LibBoostAsioConnectionImpl() = default;

private:
	Stream _stream;
	boost::asio::strand<executor_type> _strand;
	boost::asio::steady_timer _heartbeat;
	Connection _connection;
	boost::asio::streambuf _rx_streambuf;
};

template<class Stream>
class LibBoostAsioConnection {
private:
	Connection& connection() {
		return _implementation->connection();
	}

	const Connection& connection() const {
		return _implementation->connection();
	}

public:
	using executor_type = typename LibBoostAsioConnectionImpl<Stream>::executor_type;
	using next_layer_type = typename LibBoostAsioConnectionImpl<Stream>::next_layer_type;
	using lowest_layer_type = typename LibBoostAsioConnectionImpl<Stream>::lowest_layer_type;

	LibBoostAsioConnection(Stream&& stream, const Login &login, const std::string &vhost):
		_implementation{LibBoostAsioConnectionImpl<Stream>::create(std::forward<Stream>(stream), login, vhost)} {}
	LibBoostAsioConnection(Stream&& stream, const Login &login):
		_implementation{LibBoostAsioConnectionImpl<Stream>::create(std::forward<Stream>(stream), login)} {}
	LibBoostAsioConnection(Stream&& stream, const std::string &vhost):
		_implementation{LibBoostAsioConnectionImpl<Stream>::create(std::forward<Stream>(stream), vhost)} {}
	LibBoostAsioConnection(Stream&& stream):
		_implementation{LibBoostAsioConnectionImpl<Stream>::create(std::forward<Stream>(stream))} {}

	const executor_type& get_executor() noexcept {
		return _implementation->get_executor();
	}

	lowest_layer_type& lowest_layer() noexcept {
		return _implementation->lowest_layer();
	}

	const lowest_layer_type& lowest_layer() const noexcept {
		return _implementation->lowest_layer();
	}

	next_layer_type& next_layer() noexcept {
		return _implementation->next_layer();
	}

	const next_layer_type& next_layer() const noexcept {
		return _implementation->next_layer();
	}

	const Login& login() const {
		return connection().login();
	}

	const std::string& vhost() const {
		return connection().vhost();
	}

	uint32_t maxFrame() const {
		return connection().maxFrame();
	}

	uint32_t expected() const {
		return connection().expected();
	}

	bool ready() const {
		return connection().ready();
	}

	bool initialized() const {
		return connection().initialized();
	}

	bool usable() const {
		return connection().usable();
	}

	bool close() {
		return connection().close();
	}

	std::size_t channels() const {
		return connection().channels();
	}

	bool waiting() const {
		return connection().waiting();
	}

	friend LibBoostAsioChannel;

private:
	std::shared_ptr<LibBoostAsioConnectionImpl<Stream>> _implementation;
};

class LibBoostAsioChannel: public Channel {
public:
	template<class Stream>
	LibBoostAsioChannel(LibBoostAsioConnection<Stream>* connection):
		Channel(&connection->connection()) {}

	virtual ~LibBoostAsioChannel() = default;
};

} // AMQP
