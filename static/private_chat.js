(function () {
  "use strict";

  const page = document.querySelector(
    "[data-private-chat-page]"
  );

  if (!page) {
    return;
  }

  const roomId = String(
    page.dataset.roomId || ""
  );

  const currentUserId = String(
    page.dataset.currentUserId || ""
  );

  const profileUrlTemplate =
    page.dataset.profileUrlTemplate || "";

  const messageList =
    document.getElementById(
      "private-chat-messages"
    );

  const chatForm =
    document.getElementById(
      "private-chat-form"
    );

  const chatInput =
    document.getElementById(
      "private-chat-input"
    );

  const errorElement =
    document.getElementById(
      "private-chat-error"
    );

  const lengthElement =
    document.getElementById(
      "private-chat-length"
    );

  const sendButton =
    document.getElementById(
      "private-chat-send"
    );

  if (
    !roomId ||
    !currentUserId ||
    !messageList ||
    !chatForm ||
    !chatInput ||
    !errorElement ||
    !lengthElement ||
    !sendButton
  ) {
    console.error(
      "1:1 채팅 화면에 필요한 요소를 찾지 못했습니다."
    );

    return;
  }

  function formatTime(value) {
    const date = new Date(value);

    if (Number.isNaN(date.getTime())) {
      return "";
    }

    return date.toLocaleTimeString(
      "ko-KR",
      {
        hour: "2-digit",
        minute: "2-digit",
        hour12: false
      }
    );
  }

  function updateComposer() {
    const count = chatInput.value.length;
    const message = chatInput.value.trim();
    const overLimit = count > 500;

    lengthElement.textContent =
      `${count} / 500`;

    lengthElement.classList.toggle(
      "over-limit",
      overLimit
    );

    if (overLimit) {
      errorElement.textContent =
        "메시지는 최대 500자까지 전송할 수 있습니다.";
    } else if (
      errorElement.dataset.connectionError !== "true"
    ) {
      errorElement.textContent = "";
    }

    sendButton.disabled =
      message.length === 0 || overLimit;
  }

  function createProfileUrl(userId) {
    return profileUrlTemplate.replace(
      "__USER_ID__",
      encodeURIComponent(userId)
    );
  }

  function appendMessage(data) {
    const senderId = String(
      data.sender_id || ""
    );

    const isOwnMessage =
      senderId === currentUserId;

    const messageRow =
      document.createElement("div");

    messageRow.className =
      `private-message-row ${
        isOwnMessage ? "own" : "other"
      }`;

    messageRow.dataset.messageId =
      String(data.message_id || "");

    const messageContent =
      document.createElement("div");

    messageContent.className =
      "private-message-content";

    if (!isOwnMessage) {
      const senderLink =
        document.createElement("a");

      senderLink.className =
        "private-message-sender";

      senderLink.href =
        createProfileUrl(senderId);

      senderLink.textContent =
        data.sender_name || "사용자";

      messageContent.appendChild(
        senderLink
      );
    }

    const messageBubble =
      document.createElement("div");

    messageBubble.className =
      "private-message-bubble";

    messageBubble.textContent =
      String(data.message || "");

    const messageMeta =
      document.createElement("div");

    messageMeta.className =
      "private-message-meta";

    if (isOwnMessage) {
      const readState =
        document.createElement("span");

      readState.className =
        "private-read-state";

      readState.textContent =
        data.read_at ? "읽음" : "안읽음";

      messageMeta.appendChild(readState);
      messageMeta.append(" · ");
    }

    messageMeta.append(
      formatTime(data.created_at)
    );

    messageContent.appendChild(
      messageBubble
    );

    messageContent.appendChild(
      messageMeta
    );

    messageRow.appendChild(
      messageContent
    );

    messageList.appendChild(
      messageRow
    );

    messageList.scrollTop =
      messageList.scrollHeight;
  }

  chatInput.addEventListener(
    "input",
    updateComposer
  );

  chatInput.addEventListener(
    "keydown",
    function (event) {
      if (
        event.key === "Enter" &&
        !event.shiftKey
      ) {
        event.preventDefault();

        if (!sendButton.disabled) {
          chatForm.requestSubmit();
        }
      }
    }
  );

  updateComposer();

  messageList.scrollTop =
    messageList.scrollHeight;

  if (typeof window.io !== "function") {
    errorElement.textContent =
      "채팅 모듈을 불러오지 못했습니다.";

    errorElement.dataset.connectionError =
      "true";

    return;
  }

  const socket = window.io();

  socket.on(
    "connect",
    function () {
      errorElement.dataset.connectionError =
        "false";

      errorElement.textContent = "";

      socket.emit(
        "join_private_room",
        {
          room_id: roomId
        }
      );

      updateComposer();
    }
  );

  socket.on(
    "connect_error",
    function () {
      errorElement.textContent =
        "채팅 서버에 연결하지 못했습니다.";

      errorElement.dataset.connectionError =
        "true";
    }
  );

  chatForm.addEventListener(
    "submit",
    function (event) {
      event.preventDefault();

      const message =
        chatInput.value.trim();

      if (!message) {
        errorElement.textContent =
          "메시지를 입력해주세요.";

        return;
      }

      if (message.length > 500) {
        errorElement.textContent =
          "메시지는 최대 500자까지 전송할 수 있습니다.";

        return;
      }

      if (!socket.connected) {
        errorElement.textContent =
          "채팅 서버에 연결 중입니다. 잠시 후 다시 시도해주세요.";

        errorElement.dataset.connectionError =
          "true";

        return;
      }

      socket.emit(
        "send_private_message",
        {
          room_id: roomId,
          message: message
        }
      );

      chatInput.value = "";

      errorElement.dataset.connectionError =
        "false";

      updateComposer();
      chatInput.focus();
    }
  );

  socket.on(
    "private_message",
    function (data) {
      if (
        !data ||
        String(data.room_id) !== roomId
      ) {
        return;
      }

      appendMessage(data);

      if (
        String(data.sender_id) !==
        currentUserId
      ) {
        socket.emit(
          "join_private_room",
          {
            room_id: roomId
          }
        );
      }
    }
  );

  socket.on(
    "private_messages_read",
    function (data) {
      if (
        !data ||
        String(data.room_id) !== roomId ||
        String(data.reader_id) ===
          currentUserId
      ) {
        return;
      }

      document.querySelectorAll(
        ".private-message-row.own .private-read-state"
      ).forEach(
        function (element) {
          element.textContent = "읽음";
        }
      );
    }
  );
    socket.on(
    "private_message_error",
    function (data) {
      const message =
        data && data.message
          ? data.message
          : "메시지를 전송할 수 없습니다.";

      errorElement.textContent =
        message;

      if (
        data &&
        data.code === "blocked"
      ) {
        chatInput.value = "";
        chatInput.disabled = true;
        sendButton.disabled = true;

        chatInput.placeholder =
          "차단 관계인 사용자와는 채팅할 수 없습니다.";
      }
    }
  );
})();