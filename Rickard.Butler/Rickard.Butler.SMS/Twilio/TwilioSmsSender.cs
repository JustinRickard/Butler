using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Twilio;
using Twilio.Rest.Api.V2010.Account;

namespace Rickard.Butler.SMS.Twilio
{
    public interface ITwilioSmsSender
    {
        MessageResource Send(SmsMessage message);
        Task<MessageResource> SendAsync(SmsMessage message);
    }

    public class TwilioSmsSender : ITwilioSmsSender
    {
        private TwilioOptions _options;

        public TwilioSmsSender(IOptions<TwilioOptions> options)
        {
            _options = options.Value;
        }

        public MessageResource Send(SmsMessage message)
        {
            TwilioClient.Init(_options.Username, _options.Password, _options.AccountSid);

            var response = MessageResource.Create(
                body: message.Body,
                from: message.From,
                to: message.To
            );

            return response;
        }

        public async Task<MessageResource> SendAsync(SmsMessage message)
        {
            TwilioClient.Init(_options.Username, _options.Password, _options.AccountSid);

            var response = await MessageResource.CreateAsync(
                body: message.Body,
                from: message.From,
                to: message.To
            );

            return response;
        }
    }
}
