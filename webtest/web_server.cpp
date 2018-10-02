#include "web_server.hpp"


#include "detect_ssl.hpp"
#include "server_certificate.hpp"
#include "ssl_stream.hpp"

#include <fmt/printf.h>

#include <exception>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/make_unique.hpp>
#include <boost/config.hpp>
#include <algorithm>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <deque>

using tcp = boost::asio::ip::tcp;               // from <boost/asio/ip/tcp.hpp>
namespace ssl = boost::asio::ssl;               // from <boost/asio/ssl.hpp>
namespace http = boost::beast::http;            // from <boost/beast/http.hpp>
namespace websocket = boost::beast::websocket;  // from <boost/beast/websocket.hpp>

namespace {
bool debug_output;
}

// Return a reasonable mime type based on the extension of a file.
boost::beast::string_view mime_type(boost::beast::string_view path)
{
    using boost::beast::iequals;
    auto const ext = [&path] {
        auto const pos = path.rfind(".");
        if(pos == boost::beast::string_view::npos)
        {
            return boost::beast::string_view{};
        }
        return path.substr(pos);
    }();
    if(iequals(ext, ".htm"))
    {
        return "text/html";
    }
    if(iequals(ext, ".html"))
    {
        return "text/html";
    }
    if(iequals(ext, ".php"))
    {
        return "text/html";
    }
    if(iequals(ext, ".css"))
    {
        return "text/css";
    }
    if(iequals(ext, ".txt"))
    {
        return "text/plain";
    }
    if(iequals(ext, ".js"))
    {
        return "application/javascript";
    }
    if(iequals(ext, ".json"))
    {
        return "application/json";
    }
    if(iequals(ext, ".xml"))
    {
        return "application/xml";
    }
    if(iequals(ext, ".swf"))
    {
        return "application/x-shockwave-flash";
    }
    if(iequals(ext, ".flv"))
    {
        return "video/x-flv";
    }
    if(iequals(ext, ".png"))
    {
        return "image/png";
    }
    if(iequals(ext, ".jpe"))
    {
        return "image/jpeg";
    }
    if(iequals(ext, ".jpeg"))
    {
        return "image/jpeg";
    }
    if(iequals(ext, ".jpg"))
    {
        return "image/jpeg";
    }
    if(iequals(ext, ".gif"))
    {
        return "image/gif";
    }
    if(iequals(ext, ".bmp"))
    {
        return "image/bmp";
    }
    if(iequals(ext, ".ico"))
    {
        return "image/vnd.microsoft.icon";
    }
    if(iequals(ext, ".tiff"))
    {
        return "image/tiff";
    }
    if(iequals(ext, ".tif"))
    {
        return "image/tiff";
    }
    if(iequals(ext, ".svg"))
    {
        return "image/svg+xml";
    }
    if(iequals(ext, ".svgz"))
    {
        return "image/svg+xml";
    }
    return "application/text";
}

// Append an HTTP rel-path to a local filesystem path.
// The returned path is normalized for the platform.
std::string path_cat(boost::beast::string_view base, boost::beast::string_view path)
{
    if(base.empty())
    {
        return path.to_string();
    }
    std::string result = base.to_string();
#if BOOST_MSVC
    char constexpr path_separator = '\\';
    if(result.back() == path_separator)
    {
        result.resize(result.size() - 1);
    }
    result.append(path.data(), path.size());
    for(auto& c : result)
    {
        if(c == '/')
        {
            c = path_separator;
        }
    }
#else
    char constexpr path_separator = '/';
    if(result.back() == path_separator) {
        result.resize(result.size() - 1);
    }
    result.append(path.data(), path.size());
#endif
    return result;
}

class request_resolver {
public:
    virtual ~request_resolver() = default;

    virtual web_server::http_handler get_http_handler(const std::string& prefix) = 0;

    virtual web_server::ws_handler get_ws_handler(const std::string& prefix) = 0;
};

// This function produces an HTTP response for the given
// request. The type of the response object depends on the
// contents of the request, so the interface requires the
// caller to pass a generic lambda for receiving the response.
template<
        class Body, class Allocator,
        class Send>
void handle_request(boost::beast::string_view doc_root, request_resolver& req_resolver,
                    http::request<Body, http::basic_fields<Allocator>>&& req, Send&& send)
{
    // Returns a bad request response
    auto const bad_request = [&req](boost::beast::string_view why) {
        http::response<http::string_body> res{http::status::bad_request, req.version()};
        res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(http::field::content_type, "text/html");
        res.keep_alive(req.keep_alive());
        res.body() = why.to_string();
        res.prepare_payload();
        return res;
    };

    // Returns a not found response
    auto const not_found = [&req](boost::beast::string_view target) {
        http::response<http::string_body> res{http::status::not_found, req.version()};
        res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(http::field::content_type, "text/html");
        res.keep_alive(req.keep_alive());
        res.body() = "The resource '" + target.to_string() + "' was not found.";
        res.prepare_payload();
        return res;
    };

    // Returns a server error response
    auto const server_error = [&req](boost::beast::string_view what) {
        http::response<http::string_body> res{http::status::internal_server_error, req.version()};
        res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(http::field::content_type, "text/html");
        res.keep_alive(req.keep_alive());
        res.body() = "An error occurred: '" + what.to_string() + "'";
        res.prepare_payload();
        return res;
    };

    // Make sure we can handle the method
    if(req.method() != http::verb::get && req.method() != http::verb::head)
    {
        send(bad_request("Unknown HTTP-method"));
        return;
    }

    // Request path must be absolute and not contain "..".
    if(req.target().empty() || req.target()[0] != '/' || req.target().find("..") != boost::beast::string_view::npos)
    {
        send(bad_request("Illegal request-target"));
        return;
    }


    // Build the path to the requested file
    std::string path;
    std::string data;
    std::string contentType;

    auto hnd = req_resolver.get_http_handler(req.target().to_string());
    if(hnd)
    {
        std::string error;
        bool notFound = false;
        struct http_responder_impl : public http_responder {
            std::string& path;
            std::string& data;
            std::string& error;
            std::string& contentType;
            bool& notFound;

            http_responder_impl(std::string& argPath, std::string& argData, std::string& argError,
                                std::string& argContentType, bool& argNotFound) :
                    path(argPath), data(argData), error(argError), contentType(argContentType), notFound(argNotFound)
            {
            }

            void respondWithNotFound() override
            {
                notFound = true;
            }

            void respondWithError(const std::string& message) override
            {
                error = message;
            }

            void respondWithFile(const std::string& argPath, const std::string& argContentType = {}) override
            {
                path = argPath;
                contentType = argContentType;
            }

            void respondWithText(const std::string& argData, const std::string& argContentType)
            {
                data = argData;
                contentType = argContentType;
            }
        };
        http_responder_impl responder(path, data, error, contentType, notFound);
        hnd(req.target().to_string(), responder);
        if(notFound)
        {
            not_found(req.target());
            return;
        }
        if(!error.empty())
        {
            server_error(error);
            return;
        }
    }
    else
    {
        path = path_cat(doc_root, req.target());
    }
    if(req.target().back() == '/')
    {
        path.append("index.html");
    }

    // Attempt to open the file
    http::file_body::value_type body;
    if(data.empty())
    {
        boost::beast::error_code ec;
        body.open(path.c_str(), boost::beast::file_mode::scan, ec);
        // Handle the case where the file doesn't exist
        if(ec == boost::system::errc::no_such_file_or_directory)
        {
            return send(not_found(req.target()));
        }

        // Handle an unknown error
        if(ec)
        {
            return send(server_error(ec.message()));
        }
        if(contentType.empty())
        {
            contentType = mime_type(path).to_string();
        }
    }

    // Cache the size since we need it after the move
    auto const size = data.empty() ? body.size() : data.size();

    // Respond to HEAD request
    if(req.method() == http::verb::head)
    {
        http::response<http::empty_body> res{http::status::ok, req.version()};
        res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(http::field::content_type, contentType);
        res.content_length(size);
        res.keep_alive(req.keep_alive());
        send(std::move(res));
        return;
    }

    // Respond to GET request
    if(data.empty())
    {
        http::response<http::file_body> res{
                std::piecewise_construct,
                std::make_tuple(std::move(body)),
                std::make_tuple(http::status::ok, req.version())};
        res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(http::field::content_type, contentType);
        res.content_length(size);
        res.keep_alive(req.keep_alive());
        send(std::move(res));
    }
    else
    {
        http::response<http::string_body> res{
                std::piecewise_construct,
                std::make_tuple(std::move(data)),
                std::make_tuple(http::status::ok, req.version())};
        res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(http::field::content_type, contentType);
        res.content_length(size);
        res.keep_alive(req.keep_alive());
        send(std::move(res));
    }
}

//------------------------------------------------------------------------------

// Report a failure
void
fail(boost::system::error_code ec, char const* what)
{
    std::cerr << what << ": " << ec.message() << "\n";
}

//------------------------------------------------------------------------------

// Echoes back all received WebSocket messages.
// This uses the Curiously Recurring Template Pattern so that
// the same code works with both SSL streams and regular sockets.
template<class Derived>
class websocket_session {
    // Access the derived class, this is part of
    // the Curiously Recurring Template Pattern idiom.
    Derived& derived()
    {
        return static_cast<Derived&>(*this);
    }

    boost::beast::multi_buffer m_buffer;
    std::string m_response;
    char m_ping_state = 0;

protected:
    boost::asio::strand<boost::asio::io_context::executor_type> m_strand;
    boost::asio::steady_timer m_timer;
    web_server::ws_handler m_req_handler;

public:
    // Construct the session
    explicit
    websocket_session(boost::asio::io_context& ioc, web_server::ws_handler req_handler)
            : m_strand(ioc.get_executor()), m_timer(ioc, (std::chrono::steady_clock::time_point::max) ()),
              m_req_handler(std::move(req_handler))
    {
    }

    ~websocket_session()
    {
        if(debug_output)
        {
            printf("~websocket_session()\n");
        }
    }

    // Start the asynchronous operation
    template<class Body, class Allocator>
    void
    do_accept(http::request<Body, http::basic_fields<Allocator>> req)
    {
        // Set the control callback. This will be called
        // on every incoming ping, pong, and close frame.
        derived().ws().control_callback(std::bind(
                &websocket_session::on_control_callback,
                this,
                std::placeholders::_1,
                std::placeholders::_2));

        // Set the timer
        m_timer.expires_after(std::chrono::seconds(15));

        // Accept the websocket handshake
        derived().ws().async_accept(
                req,
                boost::asio::bind_executor(
                        m_strand,
                        std::bind(
                                &websocket_session::on_accept,
                                derived().shared_from_this(),
                                std::placeholders::_1)));
    }

    void on_accept(boost::system::error_code ec)
    {
        // Happens when the timer closes the socket
        if(ec == boost::asio::error::operation_aborted)
        {
            return;
        }

        if(ec)
        {
            return fail(ec, "accept");
        }

        // Read a message
        do_read();
    }

    // Called when the timer expires.
    void on_timer(boost::system::error_code ec)
    {
        if(ec && ec != boost::asio::error::operation_aborted)
        {
            return fail(ec, "timer");
        }

        // See if the timer really expired since the deadline may have moved.
        if(m_timer.expiry() <= std::chrono::steady_clock::now())
        {
            // If this is the first time the timer expired,
            // send a ping to see if the other end is there.
            if(derived().ws().is_open() && m_ping_state == 0)
            {
                // Note that we are sending a ping
                m_ping_state = 1;

                // Set the timer
                m_timer.expires_after(std::chrono::seconds(15));

                // Now send the ping
                derived().ws().async_ping({},
                        boost::asio::bind_executor(
                                m_strand,
                                std::bind(
                                        &websocket_session::on_ping,
                                        derived().shared_from_this(),
                                        std::placeholders::_1)));
            }
            else
            {
                // The timer expired while trying to handshake,
                // or we sent a ping and it never completed or
                // we never got back a control frame, so close.

                derived().do_timeout();
                return;
            }
        }

        // Wait on the timer
        m_timer.async_wait(
                boost::asio::bind_executor(
                        m_strand,
                        std::bind(
                                &websocket_session::on_timer,
                                derived().shared_from_this(),
                                std::placeholders::_1)));
    }

    // Called to indicate activity from the remote peer
    void activity()
    {
        // Note that the connection is alive
        m_ping_state = 0;

        // Set the timer
        m_timer.expires_after(std::chrono::seconds(15));
    }

    // Called after a ping is sent.
    void on_ping(boost::system::error_code ec)
    {
        // Happens when the timer closes the socket
        if(ec == boost::asio::error::operation_aborted)
        {
            return;
        }

        if(ec)
        {
            return fail(ec, "ping");
        }

        // Note that the ping was sent.
        if(m_ping_state == 1)
        {
            m_ping_state = 2;
        }
        else
        {
            // ping_state_ could have been set to 0
            // if an incoming control frame was received
            // at exactly the same time we sent a ping.
            BOOST_ASSERT(m_ping_state == 0);
        }
    }

    void on_control_callback(websocket::frame_type kind, boost::beast::string_view payload)
    {
        boost::ignore_unused(kind, payload);

        // Note that there is activity
        activity();
    }

    void do_read()
    {
        // Read a message into our buffer
        derived().ws().async_read(
                m_buffer,
                boost::asio::bind_executor(
                        m_strand,
                        std::bind(
                                &websocket_session::on_read,
                                derived().shared_from_this(),
                                std::placeholders::_1,
                                std::placeholders::_2)));
    }

    void on_read(boost::system::error_code ec, std::size_t bytes_transferred)
    {
        boost::ignore_unused(bytes_transferred);

        // Happens when the timer closes the socket
        if(ec == boost::asio::error::operation_aborted)
        {
            return;
        }

        // This indicates that the websocket_session was closed
        if(ec == websocket::error::closed)
        {
            return;
        }

        if(ec)
        {
            fail(ec, "read");
            return;
        }

        std::string data;
        for(auto& b:m_buffer.data())
        {
            data.append(static_cast<const char*>(b.data()), b.size());
        }
        if(debug_output)
        {
            printf("in data:[%s]\n", data.c_str());
        }

        m_buffer.consume(m_buffer.size());

        struct ws_responder_impl : ws_responder {
            std::string& response;

            ws_responder_impl(std::string& argResponse) : response(argResponse)
            {
            }

            void respond(const std::string& data) override
            {
                if(debug_output)
                {
                    fmt::printf("out data:[%s], size=%u\n", data, data.size());
                    fflush(stdout);
                }
                response = data;
            }
        };

        ws_responder_impl responder(m_response);
        m_req_handler(data, responder);

        // Note that there is activity
        activity();

        // Echo the message
        derived().ws().text(derived().ws().got_text());
        derived().ws().async_write(
                boost::asio::buffer(m_response),
                boost::asio::bind_executor(
                        m_strand,
                        std::bind(
                                &websocket_session::on_write,
                                derived().shared_from_this(),
                                std::placeholders::_1,
                                std::placeholders::_2)));
    }

    void on_write(boost::system::error_code ec, std::size_t bytes_transferred)
    {
        boost::ignore_unused(bytes_transferred);

        // Happens when the timer closes the socket
        if(ec == boost::asio::error::operation_aborted)
        {
            return;
        }

        if(ec)
        {
            fail(ec, "write");
            return;
        }

        if(debug_output)
        {
            fmt::printf("bytes_transferred=%u, m_buffer.size()=%u\n", bytes_transferred, m_buffer.size());
        }

        // Clear the buffer
        m_buffer.consume(m_buffer.size());

        // Do another read
        do_read();
    }
};

// Handles a plain WebSocket connection
class plain_websocket_session
        :
                public websocket_session<plain_websocket_session>,
                public std::enable_shared_from_this<plain_websocket_session> {
    websocket::stream<tcp::socket> m_ws;
    bool m_close = false;

public:
    // Create the session
    explicit
    plain_websocket_session(tcp::socket socket, web_server::ws_handler req_handler)
            : websocket_session<plain_websocket_session>(
            socket.get_executor().context(), std::move(req_handler)), m_ws(std::move(socket))
    {
    }

    // Called by the base class
    websocket::stream<tcp::socket>&
    ws()
    {
        return m_ws;
    }

    // Start the asynchronous operation
    template<class Body, class Allocator>
    void
    run(http::request<Body, http::basic_fields<Allocator>> req)
    {
        // Run the timer. The timer is operated
        // continuously, this simplifies the code.
        on_timer({});

        // Accept the WebSocket upgrade request
        do_accept(std::move(req));
    }

    void
    do_timeout()
    {
        // This is so the close can have a timeout
        if(m_close)
        {
            return;
        }
        m_close = true;

        // Set the timer
        m_timer.expires_after(std::chrono::seconds(15));

        // Close the WebSocket Connection
        m_ws.async_close(
                websocket::close_code::normal,
                boost::asio::bind_executor(
                        m_strand,
                        std::bind(
                                &plain_websocket_session::on_close,
                                shared_from_this(),
                                std::placeholders::_1)));
    }

    void
    on_close(boost::system::error_code ec)
    {
        // Happens when close times out
        if(ec == boost::asio::error::operation_aborted)
        {
            return;
        }

        if(ec)
        {
            return fail(ec, "close");
        }

        // At this point the connection is gracefully closed
    }
};

// Handles an SSL WebSocket connection
class ssl_websocket_session
        : public websocket_session<ssl_websocket_session>, public std::enable_shared_from_this<ssl_websocket_session> {
    websocket::stream<ssl_stream<tcp::socket>> m_ws;
    boost::asio::strand<boost::asio::io_context::executor_type> m_strand;
    bool m_eof = false;

public:
    // Create the http_session
    explicit
    ssl_websocket_session(ssl_stream<tcp::socket> stream, web_server::ws_handler req_handler)
            : websocket_session<ssl_websocket_session>(
            stream.get_executor().context(), std::move(req_handler)), m_ws(std::move(stream)),
              m_strand(m_ws.get_executor())
    {
    }

    // Called by the base class
    websocket::stream<ssl_stream<tcp::socket>>& ws()
    {
        return m_ws;
    }

    // Start the asynchronous operation
    template<class Body, class Allocator>
    void run(http::request<Body, http::basic_fields<Allocator>> req)
    {
        // Run the timer. The timer is operated
        // continuously, this simplifies the code.
        on_timer({});

        // Accept the WebSocket upgrade request
        do_accept(std::move(req));
    }

    void
    do_eof()
    {
        m_eof = true;

        // Set the timer
        m_timer.expires_after(std::chrono::seconds(15));

        // Perform the SSL shutdown
        m_ws.next_layer().async_shutdown(
                boost::asio::bind_executor(
                        m_strand,
                        std::bind(
                                &ssl_websocket_session::on_shutdown,
                                shared_from_this(),
                                std::placeholders::_1)));
    }

    void
    on_shutdown(boost::system::error_code ec)
    {
        // Happens when the shutdown times out
        if(ec == boost::asio::error::operation_aborted)
        {
            return;
        }

        if(ec)
        {
            return fail(ec, "shutdown");
        }

        // At this point the connection is closed gracefully
    }

    void
    do_timeout()
    {
        // If this is true it means we timed out performing the shutdown
        if(m_eof)
        {
            return;
        }

        // Start the timer again
        m_timer.expires_at((std::chrono::steady_clock::time_point::max) ());
        on_timer({});
        do_eof();
    }
};

template<class Body, class Allocator>
void
make_websocket_session(tcp::socket socket,
                       http::request<Body,
                               http::basic_fields<Allocator>> req,
                       web_server::ws_handler req_handler
)
{
    std::make_shared<plain_websocket_session>(std::move(socket), std::move(req_handler))->run(std::move(req));
}

template<class Body, class Allocator>
void make_websocket_session(ssl_stream<tcp::socket> stream,
                            http::request<Body,
                                    http::basic_fields<Allocator>> req,
                            web_server::ws_handler req_handler)
{
    std::make_shared<ssl_websocket_session>(std::move(stream), std::move(req_handler))->run(std::move(req));
}

//------------------------------------------------------------------------------

// Handles an HTTP server connection.
// This uses the Curiously Recurring Template Pattern so that
// the same code works with both SSL streams and regular sockets.
template<class Derived>
class http_session {
    // Access the derived class, this is part of
    // the Curiously Recurring Template Pattern idiom.
    Derived& derived()
    {
        return static_cast<Derived&>(*this);
    }

    // This queue is used for HTTP pipelining.
    class queue {
        enum {
            // Maximum number of responses we will queue
                    limit = 8
        };

        // The type-erased, saved work item
        struct work {
            virtual ~work() = default;

            virtual void operator()() = 0;
        };

        http_session& m_self;
        std::deque<std::unique_ptr<work>> m_items;

    public:
        explicit
        queue(http_session& self)
                : m_self(self)
        {
            static_assert(limit > 0, "queue limit must be positive");
        }

        // Returns `true` if we have reached the queue limit
        bool is_full() const
        {
            return m_items.size() >= limit;
        }

        // Called when a message finishes sending
        // Returns `true` if the caller should initiate a read
        bool on_write()
        {
            BOOST_ASSERT(!m_items.empty());
            auto const was_full = is_full();
            m_items.erase(m_items.begin());
            if(!m_items.empty())
            {
                (*m_items.front())();
            }
            return was_full;
        }

        // Called by the HTTP handler to send a response.
        template<bool isRequest, class Body, class Fields>
        void operator()(http::message<isRequest, Body, Fields>&& msg)
        {
            // This holds a work item
            struct work_impl : work {
                http_session& m_self;
                http::message<isRequest, Body, Fields> m_msg;

                work_impl(
                        http_session& self,
                        http::message<isRequest, Body, Fields>&& msg) :
                        m_self(self), m_msg(std::move(msg))
                {
                }

                void
                operator()()
                {
                    http::async_write(
                            m_self.derived().stream(),
                            m_msg,
                            boost::asio::bind_executor(
                                    m_self.m_strand,
                                    std::bind(
                                            &http_session::on_write,
                                            m_self.derived().shared_from_this(),
                                            std::placeholders::_1,
                                            m_msg.need_eof())));
                }
            };

            // Allocate and store the work
            m_items.push_back(boost::make_unique<work_impl>(m_self, std::move(msg)));

            // If there was no previous work, start this one
            if(m_items.size() == 1)
            {
                (*m_items.front())();
            }
        }
    };

    std::string const& m_doc_root;
    request_resolver& m_req_resolver;
    http::request<http::string_body> m_req;
    queue m_queue;

protected:
    boost::asio::steady_timer m_timer;
    boost::asio::strand<boost::asio::io_context::executor_type> m_strand;
    boost::beast::flat_buffer m_buffer;

public:
    // Construct the session
    http_session(
            boost::asio::io_context& ioc,
            boost::beast::flat_buffer buffer,
            std::string const& doc_root,
            request_resolver& req_resolver)
            : m_doc_root(doc_root), m_req_resolver(req_resolver), m_queue(*this),
              m_timer(ioc, (std::chrono::steady_clock::time_point::max) ()), m_strand(ioc.get_executor()),
              m_buffer(std::move(buffer))
    {
        if(debug_output)
        {
            printf("[%p]http_session()\n", this);
        }
    }

    ~http_session()
    {
        if(debug_output)
        {
            printf("[%p]~http_session()\n", this);
        }
    }

    void do_read()
    {
        // Set the timer
        m_timer.expires_after(std::chrono::seconds(15));

        // Make the request empty before reading,
        // otherwise the operation behavior is undefined.
        m_req = {};

        // Read a request
        http::async_read(
                derived().stream(),
                m_buffer,
                m_req,
                boost::asio::bind_executor(
                        m_strand,
                        std::bind(
                                &http_session::on_read,
                                derived().shared_from_this(),
                                std::placeholders::_1)));
    }

    // Called when the timer expires.
    void on_timer(boost::system::error_code ec)
    {
        if(ec && ec != boost::asio::error::operation_aborted)
        {
            return fail(ec, "timer");
        }

        // Verify that the timer really expired since the deadline may have moved.
        if(m_timer.expiry() <= std::chrono::steady_clock::now())
        {
            derived().do_timeout();
            return;
        }

        // Wait on the timer
        m_timer.async_wait(
                boost::asio::bind_executor(
                        m_strand,
                        std::bind(
                                &http_session::on_timer,
                                derived().shared_from_this(),
                                std::placeholders::_1)));
    }

    void on_read(boost::system::error_code ec)
    {
        // Happens when the timer closes the socket
        if(ec == boost::asio::error::operation_aborted)
        {
            return;
        }

        // This means they closed the connection
        if(ec == http::error::end_of_stream)
        {
            derived().do_eof();
            return;
        }

        if(ec)
        {
            return fail(ec, "read");
        }

        // See if it is a WebSocket Upgrade
        if(websocket::is_upgrade(m_req))
        {
            auto res = m_req_resolver.get_ws_handler(m_req.target().to_string());
            if(res)
            {
                make_websocket_session(
                        derived().release_stream(),
                        std::move(m_req),
                        std::move(res));
                return;
            }
        }

        // Send the response
        handle_request(m_doc_root, m_req_resolver, std::move(m_req), m_queue);

        // If we aren't at the queue limit, try to pipeline another request
        if(!m_queue.is_full())
        {
            do_read();
        }
    }

    void on_write(boost::system::error_code ec, bool close)
    {
        // Happens when the timer closes the socket
        if(ec == boost::asio::error::operation_aborted)
        {
            return;
        }

        if(ec)
        {
            return fail(ec, "write");
        }

        if(close)
        {
            // This means we should close the connection, usually because
            // the response indicated the "Connection: close" semantic.
            derived().do_eof();
            return;
        }

        // Inform the queue that a write completed
        if(m_queue.on_write())
        {
            // Read another request
            do_read();
        }
    }
};

// Handles a plain HTTP connection
class plain_http_session
        : public http_session<plain_http_session>, public std::enable_shared_from_this<plain_http_session> {
    tcp::socket m_socket;
    boost::asio::strand<boost::asio::io_context::executor_type> m_strand;

public:
    // Create the http_session
    plain_http_session(
            tcp::socket socket,
            boost::beast::flat_buffer buffer,
            std::string const& doc_root,
            request_resolver& req_resolver)
            : http_session<plain_http_session>(
            socket.get_executor().context(),
            std::move(buffer),
            doc_root,
            req_resolver), m_socket(std::move(socket)), m_strand(m_socket.get_executor())
    {
    }

    // Called by the base class
    tcp::socket& stream()
    {
        return m_socket;
    }

    // Called by the base class
    tcp::socket release_stream()
    {
        return std::move(m_socket);
    }

    // Start the asynchronous operation
    void run()
    {
        // Run the timer. The timer is operated
        // continuously, this simplifies the code.
        on_timer({});

        do_read();
    }

    void do_eof()
    {
        // Send a TCP shutdown
        boost::system::error_code ec;
        m_socket.shutdown(tcp::socket::shutdown_send, ec);

        // At this point the connection is closed gracefully
    }

    void do_timeout()
    {
        // Closing the socket cancels all outstanding operations. They
        // will complete with boost::asio::error::operation_aborted
        boost::system::error_code ec;
        m_socket.shutdown(tcp::socket::shutdown_both, ec);
        m_socket.close(ec);
    }
};

// Handles an SSL HTTP connection
class ssl_http_session
        : public http_session<ssl_http_session>, public std::enable_shared_from_this<ssl_http_session> {
    ssl_stream<tcp::socket> m_stream;
    bool m_released = false;
    boost::asio::strand<boost::asio::io_context::executor_type> m_strand;
    bool m_eof = false;

public:
    // Create the http_session
    ssl_http_session(
            tcp::socket socket,
            ssl::context& ctx,
            boost::beast::flat_buffer buffer,
            std::string const& doc_root,
            request_resolver& req_resolver)
            : http_session<ssl_http_session>(
            socket.get_executor().context(),
            std::move(buffer),
            doc_root,
            req_resolver), m_stream(std::move(socket), ctx), m_strand(m_stream.get_executor())
    {
    }

    // Called by the base class
    ssl_stream<tcp::socket>& stream()
    {
        return m_stream;
    }

    // Called by the base class
    ssl_stream<tcp::socket> release_stream()
    {
        m_released = true;
        return std::move(m_stream);
    }

    // Start the asynchronous operation
    void run()
    {
        // Run the timer. The timer is operated
        // continuously, this simplifies the code.
        on_timer({});

        // Set the timer
        m_timer.expires_after(std::chrono::seconds(15));

        // Perform the SSL handshake
        // Note, this is the buffered version of the handshake.
        m_stream.async_handshake(
                ssl::stream_base::server,
                m_buffer.data(),
                boost::asio::bind_executor(
                        m_strand,
                        std::bind(
                                &ssl_http_session::on_handshake,
                                shared_from_this(),
                                std::placeholders::_1,
                                std::placeholders::_2)));
    }

    void on_handshake(boost::system::error_code ec, std::size_t bytes_used)
    {
        // Happens when the handshake times out
        if(ec == boost::asio::error::operation_aborted)
        {
            return;
        }

        if(ec)
        {
            return fail(ec, "handshake");
        }

        // Consume the portion of the buffer used by the handshake
        m_buffer.consume(bytes_used);

        do_read();
    }

    void
    do_eof()
    {
        m_eof = true;

        // Set the timer
        m_timer.expires_after(std::chrono::seconds(15));

        // Perform the SSL shutdown
        if(!m_released)
        {
            m_stream.async_shutdown(
                    boost::asio::bind_executor(
                            m_strand,
                            std::bind(
                                    &ssl_http_session::on_shutdown,
                                    shared_from_this(),
                                    std::placeholders::_1)));
        }
    }

    void
    on_shutdown(boost::system::error_code ec)
    {
        // Happens when the shutdown times out
        if(ec == boost::asio::error::operation_aborted)
        {
            return;
        }

        if(ec)
        {
            return fail(ec, "shutdown");
        }

        // At this point the connection is closed gracefully
    }

    void
    do_timeout()
    {
        // If this is true it means we timed out performing the shutdown
        if(m_eof)
        {
            return;
        }

        // Start the timer again
        m_timer.expires_at((std::chrono::steady_clock::time_point::max) ());
        on_timer({});
        do_eof();
    }
};

//------------------------------------------------------------------------------

// Detects SSL handshakes
class detect_session : public std::enable_shared_from_this<detect_session> {
    tcp::socket m_socket;
    ssl::context& m_ctx;
    boost::asio::strand<boost::asio::io_context::executor_type> m_strand;
    std::string const& m_doc_root;
    boost::beast::flat_buffer m_buffer;

    request_resolver& m_req_resolver;
public:
    explicit
    detect_session(
            tcp::socket socket,
            ssl::context& ctx,
            std::string const& doc_root,
            request_resolver& req_resolver)
            : m_socket(std::move(socket)), m_ctx(ctx), m_strand(m_socket.get_executor()), m_doc_root(doc_root),
              m_req_resolver(req_resolver)
    {
    }

    // Launch the detector
    void run()
    {
        async_detect_ssl(
                m_socket,
                m_buffer,
                boost::asio::bind_executor(
                        m_strand,
                        std::bind(
                                &detect_session::on_detect,
                                shared_from_this(),
                                std::placeholders::_1,
                                std::placeholders::_2)));

    }

    void
    on_detect(boost::system::error_code ec, boost::tribool result)
    {
        if(ec)
        {
            return fail(ec, "detect");
        }

        if(result)
        {
            // Launch SSL session
            std::make_shared<ssl_http_session>(
                    std::move(m_socket),
                    m_ctx,
                    std::move(m_buffer),
                    m_doc_root,
                    m_req_resolver)->run();
            return;
        }

        // Launch plain session
        std::make_shared<plain_http_session>(
                std::move(m_socket),
                std::move(m_buffer),
                m_doc_root,
                m_req_resolver)->run();
    }
};

// Accepts incoming connections and launches the sessions
class listener : public std::enable_shared_from_this<listener> {
    ssl::context& m_ctx;
    tcp::acceptor m_acceptor;
    tcp::socket m_socket;
    const std::string& m_doc_root;

    request_resolver& m_req_resolver;

public:
    listener(
            boost::asio::io_context& ioc,
            ssl::context& ctx,
            tcp::endpoint endpoint,
            const std::string& doc_root,
            request_resolver& req_resolver)
            : m_ctx(ctx), m_acceptor(ioc), m_socket(ioc), m_doc_root(doc_root), m_req_resolver(req_resolver)
    {
        boost::system::error_code ec;

        // Open the acceptor
        m_acceptor.open(endpoint.protocol(), ec);
        if(ec)
        {
            fail(ec, "open");
            return;
        }

        // Allow address reuse
        m_acceptor.set_option(boost::asio::socket_base::reuse_address(true));
        if(ec)
        {
            fail(ec, "set_option");
            return;
        }

        // Bind to the server address
        m_acceptor.bind(endpoint, ec);
        if(ec)
        {
            fail(ec, "bind");
            return;
        }

        // Start listening for connections
        m_acceptor.listen(boost::asio::socket_base::max_listen_connections, ec);
        if(ec)
        {
            fail(ec, "listen");
            return;
        }
    }

    // Start accepting incoming connections
    void run()
    {
        if(!m_acceptor.is_open())
        {
            return;
        }
        do_accept();
    }

    void do_accept()
    {
        m_acceptor.async_accept(
                m_socket,
                std::bind(
                        &listener::on_accept,
                        shared_from_this(),
                        std::placeholders::_1));
    }

    void on_accept(boost::system::error_code ec)
    {
        if(ec)
        {
            fail(ec, "accept");
        }
        else
        {
            // Create the detector http_session and run it
            std::make_shared<detect_session>(
                    std::move(m_socket),
                    m_ctx,
                    m_doc_root,
                    m_req_resolver)->run();
        }

        // Accept another connection
        do_accept();
    }
};


struct web_server::web_server_impl : public request_resolver {
    boost::asio::ssl::context m_ctx;
    boost::asio::io_context m_io;
    boost::asio::signal_set m_signals;
    std::vector<std::thread> m_threads;

    std::map<std::string, web_server::http_handler> m_http_handlers;
    std::map<std::string, web_server::ws_handler> m_ws_handlers;

    web_server_impl(size_t argThreads) : m_ctx(ssl::context::sslv23), m_io(argThreads), m_signals(m_io, SIGINT, SIGTERM)
    {
        m_threads.resize(argThreads - 1);
    }

    bool init(const std::string& argAddress, uint16_t port, const std::string& webroot)
    {
        const auto address = boost::asio::ip::make_address(argAddress);

        m_signals.async_wait(
                [this](boost::system::error_code const&, int) {
                    // Stop the `io_context`. This will cause `run()`
                    // to return immediately, eventually destroying the
                    // `io_context` and all of the sockets in it.
                    m_io.stop();
                });
        load_server_certificate(m_ctx);
        std::make_shared<listener>(
                m_io,
                m_ctx,
                tcp::endpoint{address, port},
                webroot,
                *this)->run();

        return true;
    }

    void run()
    {
        for(auto& t : m_threads)
        {
            t = std::thread([this]() {
                m_io.run();
            });
        }
        m_io.run();
    }

    void shutdown()
    {
        for(auto& t : m_threads)
        {
            if(t.joinable())
            {
                t.join();
            }
        }
    }

    void registerHttpHandler(const std::string& prefix, http_handler hnd)
    {
        m_http_handlers.emplace(prefix, std::move(hnd));
    }

    void registerWsHandler(const std::string& prefix, ws_handler hnd)
    {
        m_ws_handlers.emplace(prefix, std::move(hnd));
    }

    web_server::http_handler get_http_handler(const std::string& prefix) override
    {
        auto p = m_http_handlers.equal_range(prefix);
        auto b = p.first;
        if(b != m_http_handlers.begin())
        {
            --b;
        }
        for(auto it = b; it != p.second; ++it)
        {
            if(prefix.compare(0, it->first.length(), it->first) == 0)
            {
                return it->second;
            }
        }
        return {};
    }

    web_server::ws_handler get_ws_handler(const std::string& prefix) override
    {
        auto it = m_ws_handlers.find(prefix);
        if(it != m_ws_handlers.end())
        {
            return it->second;
        }
        return {};
    }
};

web_server::web_server()
{
}

bool web_server::init(const config& argConfig)
{
    debug_output = argConfig.debug;
    m_impl = std::make_unique<web_server_impl>(argConfig.threads_count);
    return m_impl->init(argConfig.address, argConfig.port, argConfig.webroot);
}

void web_server::run()
{
    m_impl->run();
}

web_server::~web_server()
{
}

void web_server::shutdown()
{
    m_impl->shutdown();
}

void web_server::registerHttpHandler(const std::string& prefix, http_handler hnd)
{
    m_impl->registerHttpHandler(prefix, std::move(hnd));
}

void web_server::registerWsHandler(const std::string& prefix, ws_handler hnd)
{
    m_impl->registerWsHandler(prefix, std::move(hnd));
}
